module Nando

  class Migration
    # TODO: try and find a better way of doing this
    def set_connection(db)
      @db_connection = db
    end

    # TODO: any better place to put this method?
    def execute (sql)
      @db_connection.exec(sql)
    end

    def execute_migration (method)
      # TODO: review this is the best way of creating a transaction (don't know if re-assigning connections has weird behaviours)
      old_connection = @db_connection
      @db_connection.transaction do |conn|
        @db_connection = conn
        self.send(method)
      end
      @db_connection = old_connection
    end
  end

  class MigrationWithoutTransaction < Migration
    def execute_migration (method)
      self.send(method)
    end
  end

end