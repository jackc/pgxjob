require "rake/clean"

CLOBBER.include("tmp", "generate_migrations.sql")

directory "tmp"

def test_database
  dbname = ENV["PGXJOB_TEST_DATABASE"]
  if dbname.nil? || dbname.empty?
    puts "PGXJOB_TEST_DATABASE not set"
    exit 1
  end

  dbname
end

file "migrations/generate_migrations.sql" => FileList["migrations/*.sql"].exclude("migrations/generate_migrations.sql") do
  sh "tern", "gengen", "--version-table", "pgxjob_version", "-m", "migrations", "-o", "migrations/generate_migrations.sql"
end

file "tmp/.test_database_prepared" => FileList["tmp", "migrations/generate_migrations.sql"] do
  sh "dropdb", "--if-exists", test_database
  sh "createdb", test_database
  sh "psql", "--no-psqlrc", "--quiet", "--tuples-only", "--no-align", "-f", "migrations/generate_migrations.sql", "-o", "tmp/migrate.sql", test_database
  sh "psql", "--no-psqlrc", "--quiet", "-f", "tmp/migrate.sql", test_database
  touch "tmp/.test_database_prepared"
end

task "test:prepare" => "tmp/.test_database_prepared"

task "test:full" => "test:prepare" do
  sh "go test -race ./..."
end

task "test:short" => "test:prepare" do
  sh "go test -short ./..."
end

task default: "test:short"
