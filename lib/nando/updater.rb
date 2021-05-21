module MigrationUpdater

  def self.update_migration (migration_file_path, working_directory, functions_to_add)

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

    if !functions_to_add.nil?
      add_new_annotations_to_file_lines(functions_to_add)
    end

    @curr_migration_version, _ = NandoUtils.get_migration_version_and_name_from_file_path(migration_file_path)
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

          find_and_update_respective_down_directive(annotation_file, curr_source_file, line_match[1])

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

  def self.find_and_update_respective_down_directive (source_file, source_file_full_path, indent_space)
    # if a NANDO directive is being updated, then we need to find the respetive down (X)
    # start from the top of the file, try and find the directive (may try to optmize this later, to start at "def down") (X)
    # if the directive is found, update it.
    # if not, create one at the bottom of the file and update it
    # matching is done using the source_file, but the code to fill the down comes from previous migrations (NOT THE FILE)

    down_keyword = 'NANDO_DOWN'
    down_annotation_index = nil
    down_annotation_trigger = "(\s*)(?:#\s(?:#{down_keyword}:)(?:\s)?)(?:#{source_file})" # match 1 is the space to indent

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
        # _debug "Found matching annotation for: '#{source_file}'"
        down_annotation_index = line_index
        break
      end
    end

    # no annotation found, create one
    if down_annotation_index.nil?
      # _debug "Did not find respective down annotation for: '#{source_file}'"

      @lines.each_with_index do |line, line_index|
        # ignore before "def down"
        if line_index <= down_method_index
          next
        end

        # look for the "end" of "def down"
        line_match = line.match(down_method_end_trigger)
        if !line_match.nil?
          # _debug "Found the end of 'def down' at index: #{line_index}"
          @lines.insert(line_index, "\n") # insert empty line to keep annotations 1 line apart
          @lines.insert(line_index, indent_space + "# #{down_keyword}: #{source_file}\n")
          down_annotation_index = line_index
          break
        end
      end
    end

    # update annotation
    source_file_text = File.readlines(source_file_full_path).join(' ')
    # all capture groups are non-greedy, and include any character since names may have '.' for example
    function_info_match = /CREATE (?:OR REPLACE)? FUNCTION (.*?)\((.*?)\) RETURNS (.*?) AS \$\w*\$/im.match(source_file_text) # case insenstive and multi-line

    if function_info_match.nil?
      raise Nando::GenericError.new("No function definition was found in '#{source_file_full_path}'")
    end

    function_name = function_info_match[1].strip
    # function_args = function_info_match[2].strip
    # function_return = function_info_match[3].strip

    file_regex = "CREATE \\(OR REPLACE\\)\\? FUNCTION #{function_name}"

    files_with_function = %x[grep -irl -e "#{file_regex}" #{NandoMigrator.working_dir}/#{NandoMigrator.migration_dir}].split("\n").sort().reverse()

    function_previous_block = nil

    for curr_file_path in files_with_function do
      # _debug curr_file_path

      if curr_file_path.include?(@curr_migration_version)
        _debug 'Ignore self while updating'
        next
      end

      curr_file_version, _ = NandoUtils.get_migration_version_and_name_from_file_path(curr_file_path)
      if curr_file_version.to_i > @curr_migration_version.to_i
        _debug 'Skipping migrations more recent than the current one'
        next
      end

      up_line_index = nil
      down_line_index = nil
      function_line_index = nil

      curr_file_lines = File.readlines(curr_file_path)

      # find up, down and line with definition
      curr_file_lines.each_with_index do |line, line_index|
        if up_line_index.nil? && line.match(/(?:\s*)def(?:\s*)up/) then up_line_index = line_index; end
        if down_line_index.nil? && line.match(/(?:\s*)def(?:\s*)down/) then down_line_index = line_index; end
        if function_line_index.nil? && line.match(/CREATE (?:OR REPLACE)? FUNCTION #{function_name}/i) then function_line_index = line_index; end

        if !up_line_index.nil? && !down_line_index.nil? && !function_line_index.nil?
          # _debug "Found all 3 lines"
          break
        end
      end

      # TODO: only catch definition between up and down indexes

      # _debug "up: #{up_line_index} | down: #{down_line_index} | function: #{function_line_index}"

      # TODO: add some validations over current block
      # TODO: match function with correct parameters/return value
      # TODO: isolate into function that extracts block

      block_indent = nil
      block_start_index = nil
      block_end_index = nil

      # get block around function
      for block_line_index in (0..function_line_index).to_a.reverse() do
        block_line = curr_file_lines[block_line_index]
        if block_match = block_line.match("(.*)update_function(?:.*)SQL(?:.*)\n")
          block_indent = block_match[1]
          block_start_index = block_line_index
          break
        end
      end

      for block_line_index in function_line_index..curr_file_lines.length do
        block_line = curr_file_lines[block_line_index]
        if block_match = block_line.match("^#{block_indent}SQL(?:.*)\n")
          block_end_index = block_line_index
          break
        end
      end

      function_block = []
      for block_line_index in block_start_index..block_end_index do
        function_block.push(curr_file_lines[block_line_index])
      end

      function_previous_block = function_block.join('')
      break

    end

    if function_previous_block.nil?
      # TODO: decide if I need to do anything more when I don't find a previous definition (like add a DROP)
      _warn "No previous definition was found for function '#{function_name}'"
      return
    end

    # erase previous block (if one exists)
    # TODO: there is similar logic above, maybe resolve to a single function
    if curr_down_block_start = @lines[down_annotation_index+1].match("(.*)update_function(.*)SQL(.*)\n")
      ending_trigger = curr_down_block_start[1] + 'SQL' + "\n"
      starting_sql_index = down_annotation_index + 1
      ending_sql_index = nil

      # find ending of block
      for ending_down_block_index in down_annotation_index+2..@lines.length-1 do
        if ending_execute_match = @lines[ending_down_block_index].match(ending_trigger)
          ending_sql_index = ending_down_block_index
          break
        end
      end

      # TODO: add protections here if it does not find the end of the block
      # delete from array lines for current update_function block
      @lines.slice!(starting_sql_index, (ending_sql_index - starting_sql_index) + 1)
    end

    @lines.insert(down_annotation_index + 1, function_previous_block)

  end

  ## adds new annotations to bottom of "up" method
  def self.add_new_annotations_to_file_lines (functions_to_add)
    migration_file_lines = @lines
    _, up_end_index, _, _ = get_migration_file_up_and_down_limits(migration_file_lines)

    # insert annotations at the bottom of the "up" method
    functions_to_add.each do |curr_function_path|
      _debug curr_function_path
      annotation = NandoUtils.get_annotation_from_file_path(curr_function_path)
      migration_file_lines.insert(up_end_index, annotation)
      migration_file_lines.insert(up_end_index, "\n") # insert empty line to separate annotations
    end

    @lines = migration_file_lines
  end

  def self.get_migration_file_up_and_down_limits (file_lines)
    up_start_index = nil
    up_end_index = nil
    down_start_index = nil
    down_end_index = nil

    curr_state = nil
    def_indent = nil

    # find up, down (beggining and end of functions are done by finding an "end" with the same indentation)
    file_lines.each_with_index do |line, line_index|
      case curr_state
      when 'up', 'down'
        # look for end of up/down
        if line_match = line.match(/^#{def_indent}end$/)
          if curr_state == 'up'
            up_end_index = line_index
          else
            down_end_index = line_index
          end
          curr_state = nil
          def_indent = nil
        end
      else
        # read line trying to find beggining of "up" or "down"
        if line_match = line.match(/(\s*)def(?:\s*)up/) then
          curr_state = 'up'
          def_indent = line_match[1]
          up_start_index = line_index
          next
        end
        if line_match = line.match(/(\s*)def(?:\s*)down/) then
          curr_state = 'down'
          def_indent = line_match[1]
          down_start_index = line_index
          next
        end
      end

      if !up_end_index.nil? && !down_end_index.nil?
        # _debug "Found up and down"
        break
      end
    end

    # TODO: might add some checks if the index values don't make sense
    return up_start_index, up_end_index, down_start_index, down_end_index
  end

end