module MigrationUpdater

  def self.update_migration (migration_file_path, working_directory)

    if !File.file?(migration_file_path)
      raise Nando::GenericError.new("No file '#{migration_file_path}' was found")
    end

    @working_directory = working_directory
    @lines = File.readlines(migration_file_path)
    up_keyword = 'NANDO'
    @up_annotation_trigger = "(\s*)(?:#\s(?:#{up_keyword}:)(?:\s)?)(.*)" # match 1 is the space to indent, and match 2 is the file being linked
    @last_scanned_index = 0

    @changed_file = false
    @source_files_copied = []

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

    annotation_file = nil
    duplicate_annotation = false

    @lines.each_with_index do |line, line_index|
      line_match = line.match(@up_annotation_trigger)

      # found a annotation that has not been updated
      if !line_match.nil? && line_index > @last_scanned_index
        @last_scanned_index = line_index
        do_another_loop = true
        annotation_file = line_match[2]

        if @source_files_copied.include?(annotation_file)
          _warn "The file '#{annotation_file}' has already been updated in the current migration, remove the duplicate annotation! Skipping!"
          duplicate_annotation = true
          break
        else
          @source_files_copied.push(annotation_file)
        end

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
      # update the block for the current annotation (if not a duplicate)
      if !(starting_sql_index.nil? && ending_sql_index.nil?) && !duplicate_annotation
        curr_source_file = "#{@working_directory}/#{annotation_file}"

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
          find_and_update_respective_down_directive(annotation_file, line_match[1])

          @last_scanned_index = starting_sql_index + curr_file_lines.length - 1
          @changed_file = true
          _success "Updated content for #{curr_source_file}"
        else
          _warn "Couldn't find file: '#{curr_source_file}'! Skipping that one!"
        end
      end
      find_and_update()
    end

  end

  def self.find_and_update_respective_down_directive (source_file_path, indent_space)
    # if a NANDO directive is being updated, then we need to find the respetive down (X)
    # start from the top of the file, try and find the directive (may try to optmize this later, to start at "def down") (X)
    # if the directive is found, update it.
    # if not, create one at the bottom of the file and update it
    # matching is done using the source_file_path, but the code to fill the down comes from previous migrations (NOT THE FILE)

    down_keyword = 'NANDO_DOWN'
    down_annotation_index = nil
    down_annotation_trigger = "(\s*)(?:#\s(?:#{down_keyword}:)(?:\s)?)(?:#{source_file_path})" # match 1 is the space to indent

    down_method_index = nil
    down_method_trigger = "(\s*)def(?:\s*)down(.*)"
    down_method_end_trigger = nil # to find respective "end"

    line_match = nil

    # find down annotation
    @lines.each_with_index do |line, line_index|
      # find start of down method (ignore before that)
      if down_method_index.nil?
        line_match = line.match(down_method_trigger)
        if !line_match.nil?
          # _debug 'Found beginning of down method'
          down_method_index = line_index
          down_method_indent = line_match[1]
          down_method_end_trigger = "^(?:#{line_match[1]}end).*"
        end
        next
      end

      # start looking for an annotation
      line_match = line.match(down_annotation_trigger)

      # found a annotation that has not been updated
      if !line_match.nil?
        # _debug "Found matching annotation for: '#{source_file_path}'"
        down_annotation_index = line_index
        break
      end
    end

    # no annotation found, create one
    if down_annotation_index.nil?
      # _debug "Did not find respective down annotation for: '#{source_file_path}'"

      @lines.each_with_index do |line, line_index|
        # ignore before "def down"
        if line_index <= down_method_index
          next
        end

        # look for the "end" of "def down"
        line_match = line.match(down_method_end_trigger)
        if !line_match.nil?
          _debug "Found the end of 'def down' at index: #{line_index}"
          @lines.insert(line_index, "\n") # insert empty line to keep annotations 1 line apart
          @lines.insert(line_index, indent_space + "# #{down_keyword}: #{source_file_path}\n")
          break
        end
      end
    end

    # update annotation - TODO

  end

end