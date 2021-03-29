class <%= migration_class_name %> < Nando::<%= migration_type %>
  def up
<%= migration_up_code %>
  end

  def down
<%= migration_down_code %>
  end
end
