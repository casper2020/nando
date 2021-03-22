class <%= migration_class_name %> < Nando::<%= migration_type %>
  def up
    execute <<-'SQL'
<%= migration_up_code %>
    SQL
  end

  def down
    execute <<-'SQL'
<%= migration_down_code %>
    SQL
  end
end
