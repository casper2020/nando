RSpec.describe Nando do
  it "has a version number" do
    expect(Nando::VERSION).not_to be nil
  end

  it "handles database URLs" do
    # handle normal URLs
    match = NandoMigrator.instance.parse_database_url('postgres://toconline:toconline@127.0.0.1:5432/toconline')
    expect(match[1]).to eq('postgres')
    expect(match[2]).to eq('toconline')
    expect(match[3]).to eq('toconline')
    expect(match[4]).to eq('127.0.0.1')
    expect(match[5]).to eq('5432')
    expect(match[6]).to eq('toconline')

    # handle URLs with "-"
    match = NandoMigrator.instance.parse_database_url('postgres://toconline-1:toconline-2@localhost:123/nando-test-migrate')
    expect(match[1]).to eq('postgres')
    expect(match[2]).to eq('toconline-1')
    expect(match[3]).to eq('toconline-2')
    expect(match[4]).to eq('localhost')
    expect(match[5]).to eq('123')
    expect(match[6]).to eq('nando-test-migrate')

    # handle URLs with "_"
    match = NandoMigrator.instance.parse_database_url('postgres://toconline_1:toconline_2@host-name_test:8080/nando_test_migrate')
    expect(match[1]).to eq('postgres')
    expect(match[2]).to eq('toconline_1')
    expect(match[3]).to eq('toconline_2')
    expect(match[4]).to eq('host-name_test')
    expect(match[5]).to eq('8080')
    expect(match[6]).to eq('nando_test_migrate')
  end

  it 'handles only specified migration types' do
    NandoMigrator.instance.camelize_migration_type('Migration')
    NandoMigrator.instance.camelize_migration_type('MigrationWithoutTransaction')

    begin
      # this should fail
      NandoMigrator.instance.camelize_migration_type('MigrationFakeType123')
    rescue => e
      expect(e.message).to eq("Invalid migration type 'MigrationFakeType123'")
    end
  end

end
