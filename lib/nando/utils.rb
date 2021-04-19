module NandoUtils

  def self.get_annotation_from_file_path (file_path)
    return "    # NANDO: #{file_path}\n"
  end

  # accepts either a path or a file name
  def self.get_migration_version_and_name_from_file_path (file_path)
    file_name = file_path.split('/')[-1] # get last part of the file path
    match = /^(\d+)\_(.*)\.rb/.match(file_name)
    if match.nil?
      raise Nando::GenericError.new("'#{file_name}' is not a valid file name")
    end
    migration_version = match[1] # by this point, the file name has already been validated, so I don't need to double check
    migration_name = match[2]
    return migration_version, migration_name
  end

  # TODO: move helper methods here, to not fill the main files

end

