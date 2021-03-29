module NandoUtils

  def self.get_migration_files (directory)
    files = Dir.children(directory)

    migration_files = []
    for filename in files do
      if !/\d\_.*\.rb$/.match(filename)
        puts "Warning: #{filename} does not have a valid migration name"
        next
      end

      migration_files.push(filename)
    end

    migration_files.sort! # sort to ensure the migrations are executed chronologically
  end

  # TODO: might merge with "get_migration_files"
  def self.get_migration_files_to_rollback (directory, versions_to_rollback)
    files = Dir.children(directory)

    migration_files = []
    for filename in files do
      match = /(\d+)\_.*\.rb$/.match(filename)
      if match[0].nil?
        # TODO: test this again for rollback
        puts "Warning: #{filename} does not have a valid migration name"
        next
      end

      if versions_to_rollback.include?(match[1])
        migration_files.push(filename)
      end
    end

    migration_files.sort.reverse # sort and reverse to ensure the migrations are executed chronologically (backwards)
  end


  
end

