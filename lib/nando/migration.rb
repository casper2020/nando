module Nando

  class Migration
    def set_connection(db)
      @db_connection = db
    end

    def execute (sql)
      @db_connection.exec(sql)
    end
  end

  class MigrationWithoutTransaction < Migration
    
  end

end