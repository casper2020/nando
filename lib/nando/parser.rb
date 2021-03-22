require 'fileutils'

module NandoParser

  def self.parse_from_dbmate (source_path, destination_path)
    source_files = Dir.children(source_path)
    source_files.sort! # this sort is not really necessary, but ensures the files are created in the same order of their name

    puts "Found #{source_files.length} source files"

    FileUtils.mkdir_p(destination_path)
    clear_directory(destination_path)

    for filename in source_files do
      match = /^(\d+)\_([\w\_]+)\./.match(filename)
      migration_version = match[1]
      migration_name = match[2]
      new_filename = "#{migration_version}_#{migration_name}.rb"
      new_file = File.new(File.join(destination_path, new_filename), 'w')

      source_file_lines = File.readlines(File.join(source_path, filename))
      current_section = nil
      up_method, down_method = '', ''
      with_transaction = true
      for line in source_file_lines do
        next if /^--[\s|\\\/_]*$/.match(line) # if it's just a up/down comment, ignore

        case current_section
        when 'up'
          if match = /--\smigrate:down(.*)/.match(line)
            with_transaction = false if match[1].include?('transaction:false')
            current_section = 'down'
          else
            up_method += "      #{line}".rstrip + "\n"
          end
        when 'down'
          down_method += "      #{line}".rstrip + "\n"
        else
          if match = /--\smigrate:up(.*)/.match(line)
            with_transaction = false if match[1].include?('transaction:false')
            current_section = 'up'
          end
        end
      end

      migration_class_name = migration_name.camelize
      migration_type = with_transaction ? Nando::Migration.name.demodulize : Nando::MigrationWithoutTransaction.name.demodulize
      migration_up_code = up_method
      migration_down_code = down_method
      # TODO: check if binding logic is correct, and if pathing changes when it's a gem
      MigrationGenerator.render_to_file(File.join(File.dirname(File.expand_path(__FILE__)), 'parser_templates/migration.rb'), binding, new_file)
    end

    dest_files = Dir.children(destination_path)
    puts "Created #{dest_files.length} migrations in the destination folder"
  end

  def self.clear_directory (path)
    Dir.foreach(path) do |f|
      fn = File.join(path, f)
      if f != '.' && f != '..'
        File.delete(fn)
      end
    end
  end

end