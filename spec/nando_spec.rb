RSpec.describe Nando do
  it "has a version number" do
    expect(Nando::VERSION).not_to be nil
  end

  it "does something useful" do
    expect(false).to eq(false)
  end

  it "has a migrate command" do
    expect(Foodie::Food.pluralize("Tomato")).to eql("Tomatoes")
  end
end
