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

    def migrate_companies (module_name = nil, options = {}, &block)
      puts 'MIGRATE COMPANIES'

      return if block.nil?

      if module_name.nil?
        work_on_schemas get_sharded_company_schemas, 'global company', :up, options, &block
        # say_with_time("[SHARDED] Running migration on sharded backup schema") { block.call 'sharded', nil, nil, 'tablespace_000' } if schema_exists?('sharded')
        # say_with_time("[PUBLIC] Running migration on unsharded companies") { block.call 'public', nil, nil, 'tablespace_000' }
      elsif module_name.to_sym == :all
        work_on_schemas get_companies_schemas, 'global company', :up, options, &block
      else
        work_on_schemas get_companies_schemas_from_module(module_name), 'global company', :up, options, &block
      end
    end


    # helper methods

    def work_on_schemas(schemas, schema_type_description, direction, options, &block)
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

    def up_on_schema(schema, running_migration_version, options, progress_feedback, &block)
      puts "UP ON SCHEMA #{progress_feedback}"
      # TODO: complete this
    end

    def down_on_schema(schema, running_migration_version, options, progress_feedback, &block)
      puts "DOWN ON SCHEMA #{progress_feedback}"
      # TODO: complete this
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
  end

end