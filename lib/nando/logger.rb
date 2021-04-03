# logger functions
require 'colorize'

def _warn (message, header = 'WARNING')
  print "#{header}: ".yellow.bold
  puts message
end

def _success (message, header = 'SUCCESS')
  print "#{header}: ".green.bold
  puts message
end

def _error (message, header = 'ERROR')
  print "#{header}: ".red.bold
  puts message
end

def _debug (message, header = 'DEBUG')
  if ENV['DEBUG'] != 'true'
    return
  end

  print "#{header}: ".light_cyan.bold
  puts message
end

def _info (message)
  puts message
end