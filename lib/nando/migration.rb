module Nando

  class Migration
    def initialize (conn)
      @conn = conn
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
    def initialize (conn)
      super(conn)
      @conn.exec('DROP FUNCTION IF EXISTS sharding.create_company_shard(integer,text)')
    end

    def execute_migration (method)
      self.send(method)
    end


    # custom CW methods

    def migrate_companies
      puts 'MIGRATE COMPANIES'
    end
  end

end