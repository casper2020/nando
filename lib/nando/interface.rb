module NandoInterface

  # prints help message
  def self.print_help_message
    schema_mig_table = "schema_migrations" # left as a variable, might fill this value dynamically later

    message = ''
    message += "Usage: nando <command> [options]\n\n"

    # commands
    commands = [
      ['up', "Executes all the migrations that are not yet on the #{schema_mig_table.white.bold} table"],
      ['down', "Rollbacks the last migration in the #{schema_mig_table.white.bold} table"],
      ['apply <version>', "Applies the migration with the specified version (even if it was already executed). Intended for development, not production"],
      ['revert <version>', "Reverts the migration with the specified version (even if it hasn't been executed). Intended for development, not production"],
      ['new <migration name>', "Creates a new migration with the specified name. Use the #{"-t/--type".white.bold} flag to specify the type of migration: #{"[Migration|MigrationWithoutTransaction]".white.bold}"],
      ['parse <source> <destination>', "Parses all the #{"dbmate".white.bold} migrations in the source folder into #{"Nando".white.bold} migrations in the destination folder"],
      ['baseline', "Creates a baseline Nando migration with all the functions currently in the database"],
      ['update <path to migration>', "Updates the specified migration. Use #{"-f/--function".white.bold} to add one or more functions to the migration file"],
      ['diff <source> <target>', "Compares 2 schemas in the database and suggests SQL commands to fix any changes found"]
    ]

    largest_command = commands.max { |a,b| a[0].length <=> b[0].length }
    required_indent = "nando #{largest_command[0]}".length

    message += "Commands:\n"
    for command in commands do
      message += build_command_message(command[0], command[1], required_indent)
    end

    # flags
    flags = [
      ['-t/--type', 'Used to specify the migration type'],
      ['-f/--function', 'Used to specify which function files to add to a migration'],
      ['-d/--dry-run', 'Used to prevent the tool from applying migrations and only provide info'],
      ['-h/--help', 'Shows the help message']
    ]

    largest_flag = flags.max { |a,b| a[0].length <=> b[0].length }
    required_indent = largest_flag[0].length

    message += "\nFlags:\n"
    for flag in flags do
      message += build_flag_message(flag[0], flag[1], required_indent)
    end

    message += "Nando Version (#{Nando::VERSION})"

    puts message
  end

  def self.build_command_message (command, description, required_indent)
    command_message = "nando #{command}"
    indent = " " * (required_indent - command_message.length)
    return "#{command_message.white.bold}#{indent}  #{description}\n"
  end

  def self.build_flag_message (flag, description, required_indent)
    indent = " " * (required_indent - flag.length)
    return "#{flag.white.bold}#{indent}  #{description}\n"
  end


  def self.get_user_function_list
    puts 'Enter the list of functions to add to the current migration: (Enter an empty line to exit)'.bold.magenta
    input = multi_line_gets
    return (input.split("\n").each { |line| line.strip! }.reject { |line| line == '' }) || []
  end

  def self.multi_line_gets (all_text = '')
    until (text = gets) == "\n"
      all_text << text
    end
    return all_text.chomp
  end

  # if input is Y/y return true, else return false
  def self.get_user_input_boolean (message)
    puts "\n#{message} (Y/N)".magenta.bold
    # TODO: review error when not using $stdin (might need to use it above as well)
    input = $stdin.gets.chomp.downcase.strip
    if input == 'y'
      return true
    else
      return false
    end
  end

end