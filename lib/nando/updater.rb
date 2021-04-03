module MigrationUpdater

  def self.update_migration (migration_file_path, working_directory)

    if !File.file?(migration_file_path)
      raise Nando::GenericError.new("No file '#{migration_file_path}' was found")
    end

    @working_directory = working_directory
    @lines = File.readlines(migration_file_path)
    @trigger_keyword = 'NANDO'
    @trigger = "(\s*)(?:#\s(?:#{@trigger_keyword}:)(?:\s)?)(.*)" # match 1 is the space to indent, and match 2 is the file being linked
    @last_scanned_index = 0

    @changed_file = false

    find_and_update()

    if @changed_file
      File.write(migration_file_path, @lines.join(''))
    end
  end

  # iterates the file, finds annotations and updates them
  def self.find_and_update
    do_another_loop = false
    prepend_append_execute = false

    starting_sql_index = ending_sql_index = nil

    line_match = nil

    execute_match = nil
    ending_execute_match = nil

    @lines.each_with_index do |line, line_index|
      line_match = line.match(@trigger)

      # found a annotation that has not been updated
      if !line_match.nil? && line_index > @last_scanned_index
        @last_scanned_index = line_index
        do_another_loop = true

        # find beginning of block
        if execute_match = @lines[line_index+1].match("(.*)update_function(.*)SQL(.*)\n")
          starting_sql_index = line_index + 1
          ending_trigger = execute_match[1] + 'SQL' + "\n"

          # find ending of block
          for ending_block_index in line_index+2..@lines.length-1 do
            if ending_execute_match = @lines[ending_block_index].match(ending_trigger)
              ending_sql_index = ending_block_index
              break
            end
          end
        # we need to create an update_function block, since one does not exist
        else
          starting_sql_index = line_index + 1
          ending_sql_index = starting_sql_index - 1
          prepend_append_execute = true
        end
        break
      end
    end

    if do_another_loop
      unless starting_sql_index.nil? && ending_sql_index.nil?
        curr_source_file = "#{@working_directory}/#{line_match[2]}"

        if File.file?(curr_source_file)
          # delete from array lines for current update_function block (if there is any)
          @lines.slice!(starting_sql_index, (ending_sql_index - starting_sql_index) + 1)
          # insert into array new update_function block
          curr_file_lines = File.readlines(curr_source_file)
          # create execute block
          if prepend_append_execute
            curr_file_lines.map! { |line| line == "\n" ? line : ("  " + line_match[1] + line) }
            curr_file_lines[curr_file_lines.length - 1].rstrip!
            curr_file_lines.insert(0, line_match[1] + "update_function <<-'SQL'\n")
            curr_file_lines.push("\n" + line_match[1] + "SQL\n")
          else
            curr_file_lines.map! { |line| line == "\n" ? line : ("  " + execute_match[1] + line) }
            curr_file_lines[curr_file_lines.length - 1].rstrip!
            curr_file_lines.insert(0, execute_match[0])
            curr_file_lines.push("\n" + ending_execute_match[0])
          end
          @lines.insert(starting_sql_index, *curr_file_lines)

          # TODO: create/update the equivalent DOWN => have a similiar directive in the down method, with the path as well, to allow for "easy" matching (?)

          @last_scanned_index = starting_sql_index + curr_file_lines.length - 1
          @changed_file = true
          _success "Updated content for #{curr_source_file}"
        else
          _warn "Couldn't find file: #{curr_source_file} => Skipping that one!"
        end
      end
      find_and_update()
    end

  end

end