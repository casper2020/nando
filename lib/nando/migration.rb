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
      puts 'MIGRATION'
      @db_connection.exec('BEGIN')
      self.send(method)
      @db_connection.exec('COMMIT')
    end
  end

  class MigrationWithoutTransaction < Migration
    def execute_migration (method)
      puts 'WITHOUT TRANSACTION'
      self.send(method)
    end
  end

end