module NandoInterface

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
    input = gets.downcase.strip.chomp
    if input == 'y'
      return true
    else
      return false
    end
  end

end