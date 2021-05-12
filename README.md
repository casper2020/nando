# Nando

Welcome to your new gem! In this directory, you'll find the files you need to be able to package up your Ruby library into a gem. Put your Ruby code in the file `lib/nando`. To experiment with that code, run `bin/console` for an interactive prompt.

TODO: Delete this and the text above, and describe your gem

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'nando'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install nando

## Usage

```nando up``` - Executes all the migrations that are not yet on the ```schema_migrations``` table

```nando down``` - Rollbacks the last migration in the ```schema_migrations``` table

```nando apply <version>``` - Applies the migration with the specified version (even if it was already executed). Intended for development, not production

```nando new <migration name>``` - Creates a new migration with the specified name. Use the ```-t/--type``` flag to specify the type of migration: ```[Migration|MigrationWithoutTransaction]```

```nando parse <source folder> <destination folder>``` - Parses all the ```dbmate``` migrations in the source folder into ```Nando``` migration in the destination folder

```nando baseline``` - Creates a baseline Nando migration with all the functions currently in the database (used to ensure ```update``` always has a migration to revert to)

```nando update <path to migration>``` - Updates the specified migration (searches for Nando annotations and updates the source code within the respective blocks). Use ```-f/--function``` to add one or more functions to the migration file

```nando diff <source schema> <target schema>``` - WIP, but compares 2 schemas in the database and suggests SQL commands to fix any changes found

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[USERNAME]/nando.
