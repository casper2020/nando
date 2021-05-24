module Nando

  class Migration
    def initialize (conn, version)
      @conn = conn
      @version = version
    end

    # TODO: any better place to put this method?
    def execute (sql)
      @conn.exec(sql)
    end

    # TODO: any better place to put this method?
    def update_function (sql)
      # TODO: add validations here
      @conn.exec(sql)
    end

    def execute_migration (method)
      # TODO: review this is the best way of creating a transaction (don't know if re-assigning connections has weird behaviours)
      old_connection = @conn
      @conn.transaction do |conn|
        @conn = conn
        self.send(method)
      end
      @conn = old_connection
    end
  end

  class MigrationWithoutTransaction < Migration
    def initialize (conn, version)
      super(conn, version)
      @conn.exec('DROP FUNCTION IF EXISTS sharding.create_company_shard(integer,text)')
    end

    def execute_migration (method)
      self.send(method)
    end


    # custom CW methods

    ###########################################################
    # WORK OVER SHARDED COMPANIES, AND ALREADY SHARDED MODULES
    ###########################################################
    def migrate_companies (module_name = nil, options = {}, &block)
      return if block.nil?

      if module_name.nil?
        work_on_schemas(get_sharded_company_schemas, 'global company', :up, options, &block)
        # TODO: splited a "say_with_time", might need to review this
        puts "[PUBLIC] Running migration on unsharded companies"
        block.call('public', nil, nil, 'tablespace_000')
      elsif module_name.to_sym == :all
        work_on_schemas(get_companies_schemas, 'global company', :up, options, &block)
      else
        work_on_schemas(get_companies_schemas_from_module(module_name), 'global company', :up, options, &block)
      end
    end

    def rollback_companies (module_name = nil, options = {}, &block)
      if module_name.nil?
        work_on_schemas(get_sharded_company_schemas, 'global company', :down, options, &block)
        # TODO: splited a "say_with_time", might need to review this
        puts "[PUBLIC] Rolling back migration on unsharded companies"
        block.call('public', nil, nil, 'tablespace_000')
      elsif module_name.to_sym == :all
        work_on_schemas(get_companies_schemas, 'global company', :down, options, &block)
      else
        work_on_schemas(get_companies_schemas_from_module(module_name), 'global company', :down, options, &block)
      end
    end


    #################################
    # WORK OVER ACCOUNTING COMPANIES
    #################################
    def migrate_accounting_companies (options = {}, &block)
      return if block.nil?

      work_on_schemas(get_accounting_companies_schemas, 'accounting company', :up, options, &block)
    end

    def rollback_accounting_companies(options = {}, &block)
      work_on_schemas(get_accounting_companies_schemas, 'accounting company', :down, options, &block)
    end

    def migrate_fiscal_years (options = {}, &block)
      return if block.nil?
      work_on_schemas get_accounting_companies_schemas, 'accounting company', :up, options do |schema, company_id, use_sharded_company, tablespace_name, company_schema|
        each_fiscal_year(schema) do |fiscal_year|
          block.call schema, fiscal_year['table_prefix'], fiscal_year, company_id, tablespace_name, company_schema, use_sharded_company
        end
      end
    end

    def rollback_fiscal_years (options = {}, &block)
      work_on_schemas get_accounting_companies_schemas, 'accounting company', :down, options do |schema, company_id, use_sharded_company, tablespace_name, company_schema|
        each_fiscal_year(schema) do |fiscal_year|
          block.call schema, fiscal_year['table_prefix'], fiscal_year, company_id, tablespace_name, company_schema, use_sharded_company
        end
      end
    end

    def each_fiscal_year (schema, &block)
      return if block.nil?
      @conn.exec(%Q[SELECT * FROM "#{schema}"."fiscal_years"]).to_a.each(&block)
    end


    ###########################
    # WORK OVER USER TEMPLATES
    ###########################
    def migrate_user_schemas (options = {}, &block)
      return if block.nil?

      work_on_schemas(get_user_schemas, 'user template', :up, options, &block)
    end

    def rollback_user_schemas (options = {}, &block)
      work_on_schemas(get_user_schemas, 'user template', :down, options, &block)
    end

    def migrate_user_templates (options = {}, &block)
      return if block.nil?

      work_on_schemas get_user_schemas, 'user template', :up, options do |schema, id, use_sharded_company, tablespace_name|
        each_user_template(schema) do |user_template|
          block.call(schema, user_template['table_prefix'], user_template, tablespace_name)
        end
      end
    end

    def rollback_user_templates (options = {}, &block)
      work_on_schemas get_user_schemas, 'user template', :down, options do |schema, id, use_sharded_company, tablespace_name|
        each_user_template(schema) do |user_template|
          block.call(schema, user_template['table_prefix'], user_template, tablespace_name)
        end
      end
    end

    def each_user_template (schema_name, &block)
      return if block.nil?
      @conn.exec(%Q[SELECT * FROM "accounting"."user_templates" WHERE schema_name='#{schema_name}' ORDER BY "id"]).to_a.each(&block)
    end

    def table_exists? (schema_name, table_name)
      @conn.exec(%Q[SELECT 1 FROM "information_schema"."tables" WHERE "table_schema" = '#{schema_name}' AND "table_name" = '#{table_name}']).any?
    end


    # helper methods

    def work_on_schemas (schemas, schema_type_description, direction, options, &block)
      options ||= {}
      options[:record_on_schema_migrations] = true unless options.has_key?(:record_on_schema_migrations)
      options[:use_public_schema] = true unless options.has_key?(:use_public_schema)

      schema_count = schemas.count.to_s
      options[:max_schemas_per_conn] ||= 200 # min_queries_per_conn / max_queries_per_conn
      options[:_internal_reset_counter] = 0

      running_migration_version = get_migration_version

      puts "#{direction == :up ? 'Migrating' : 'Rolling back'} on #{schema_count} #{schema_type_description} schema(s)"

      schemas.each_with_index do |schema, index|
        # create_schema_migrations_table_on_schema(schema) unless schema_migration_table_exists?(schema)
        send :"#{direction}_on_schema", schema, running_migration_version, options, "[#{(index + 1).to_s.rjust(schema_count.length)}/#{schema_count}] ", &block
      end
    end

    def up_on_schema (schema, running_migration_version, options, progress_feedback, &block)
      with_new_connection(options) do
        @conn.transaction do |conn|
          # TODO: ensure this has the same behavior as "ActiveRecord::Base.transaction(requires_new: true)"
          if migration_ran_on_schema?(schema, running_migration_version, conn)
            puts "#{progress_feedback}Migration already ran on schema #{schema['schema_name']}, skipping"
          else
            # TODO: splited a "say_with_time", might need to review this
            puts "#{progress_feedback}Running migration on schema #{schema['schema_name']}"
            block.call(schema['schema_name'], schema['id'], schema['use_sharded_company'].to_b, schema['tablespace_name'], schema['company_schema']) unless block.nil?
            migration_ran_on_schema!(schema, running_migration_version, conn) if options[:record_on_schema_migrations]
          end
        end
      end
    end

    def down_on_schema (schema, running_migration_version, options, progress_feedback, &block)
      with_new_connection(options) do
        @conn.transaction do |conn|
          # TODO: ensure this has the same behavior as "ActiveRecord::Base.transaction(requires_new: true)"
          if migration_ran_on_schema?(schema, running_migration_version, conn)
            # TODO: splited a "say_with_time", might need to review this
            puts "#{progress_feedback}Rolling back migration on schema #{schema['schema_name']}"
            block.call(schema['schema_name'], schema['id'], schema['use_sharded_company'].to_b, schema['tablespace_name'], schema['company_schema']) unless block.nil?
            migration_rolled_back_on_schema!(schema, running_migration_version, conn) if options[:record_on_schema_migrations]
          else
            puts "#{progress_feedback}Migration didn't run on schema #{schema['schema_name']}, skipping"
          end
        end
      end
    end

    def with_new_connection (options = {}, &block)
      options[:_internal_reset_counter] ||= 0
      if options[:_internal_reset_counter] == options[:max_schemas_per_conn] || ( 0 == options[:_internal_reset_counter] && !options[:statement_timeout].nil? )
        # TODO: what was the objective of this? Is a reset enough?
        # ActiveRecord::Base.connection.reset!()
        # ActiveRecord::Base.connection.raw_connection.reset
        # ActiveRecord::Base.connection.raw_connection.exec("SET statement_timeout TO #{0 == options[:statement_timeout]? "'48h'" : options[:statement_timeout]}") if options[:statement_timeout]
        @conn.reset()
        @conn.exec("SET statement_timeout TO #{0 == options[:statement_timeout]? "'48h'" : options[:statement_timeout]}") if options[:statement_timeout]
        options[:_internal_reset_counter] = 0
      end
      options[:_internal_reset_counter] +=1
      block.call unless block.nil?
    end


    # schema queries

    def get_sharded_company_schemas
      get_schemas %Q[
        SELECT "id",
               "schema_name",
               "use_sharded_company",
               common.get_tablespace_name("schema_name") AS "tablespace_name"
          FROM "public"."companies"
         WHERE "schema_name" IS NOT NULL
           AND "use_sharded_company"
           AND "is_deleted" IS DISTINCT FROM true
           AND "companies"."cluster" = #{database_cluster}
         ORDER BY "id"
       ]
    end

    def get_companies_schemas_from_module (module_name)
      get_schemas %Q[
        SELECT "companies"."id",
               "companies"."schema_name",
               "companies"."use_sharded_company",
               common.get_tablespace_name("companies"."schema_name") AS "tablespace_name"
          FROM "public"."company_modules"
          JOIN "public"."companies" ON "companies"."id" = "company_modules"."company_id"
         WHERE "companies"."schema_name" IS NOT NULL
           AND "company_modules"."name" = '#{module_name}'
           AND "company_modules"."has_schema_structure"
           AND "companies"."is_deleted" IS DISTINCT FROM true
           AND "companies"."cluster" = #{database_cluster}
         ORDER BY "id"
      ]
    end

    def get_companies_schemas
      get_schemas %Q[
        SELECT "id",
               "schema_name",
               "use_sharded_company",
               common.get_tablespace_name("schema_name") AS "tablespace_name"
          FROM "public"."companies"
         WHERE "schema_name" IS NOT NULL
           AND "is_deleted" IS DISTINCT FROM true
           AND "companies"."cluster" = #{database_cluster}
         ORDER BY "id"
      ]
    end

    def get_accounting_companies_schemas
      get_schemas %Q[
        SELECT "accounting_companies"."company_id" AS "id",
               "accounting_companies"."schema_name",
               common.get_tablespace_name("accounting_companies"."schema_name") AS "tablespace_name",
               "companies"."schema_name" AS company_schema,
               "companies"."use_sharded_company"
          FROM "accounting"."accounting_companies"
          JOIN "public"."companies" ON "companies"."id" = "accounting_companies"."company_id"
         WHERE "companies"."is_deleted" IS DISTINCT FROM true
           AND "companies"."cluster" = #{database_cluster}
         ORDER BY "accounting_companies"."id"
      ]
    end

    # TODO: added "true AS "use_sharded_company" here, might need to go over the NilClass.to_b problem some other way
    def get_user_schemas
      get_schemas %Q[
        SELECT DISTINCT "user_id" AS "id",
               "schema_name",
               true AS "use_sharded_company",
               common.get_tablespace_name("schema_name") AS "tablespace_name"
          FROM "accounting"."user_templates"
         ORDER BY "id"
      ]
    end


    # utils

    def database_cluster
      @conn.exec("SHOW cloudware.cluster").to_a[0]["cloudware.cluster"].to_i
    end

    def get_schemas (query)
      schemas_rows = @conn.exec(query).to_a
      return schemas_rows
    end

    def get_migration_version
      return @version
    end

    def migration_ran_on_schema? (schema, migration_version, conn = nil)
      # TODO: replace with "migration_table" env variable
      query = %Q[SELECT 1 FROM "#{schema['schema_name']}"."schema_migrations" WHERE "version" = '#{migration_version}']
      if conn.nil?
        @conn.exec(query).any?
      else
        conn.exec(query).any?
      end
    end

    def migration_ran_on_schema! (schema, migration_version, conn = nil)
      # TODO: replace with "migration_table" env variable
      query = %Q[INSERT INTO "#{schema['schema_name']}"."schema_migrations" ("version") VALUES ('#{migration_version}')]
      if conn.nil?
        @conn.exec(query)
      else
        conn.exec(query)
      end
    end

    def migration_rolled_back_on_schema! (schema, migration_version, conn = nil)
      # TODO: replace with "migration_table" env variable
      query = %Q[DELETE FROM "#{schema['schema_name']}"."schema_migrations" WHERE "version" = '#{migration_version}']
      if conn.nil?
        @conn.exec(query)
      else
        conn.exec(query)
      end
    end

  end

end