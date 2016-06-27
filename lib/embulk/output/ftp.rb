Embulk::JavaPlugin.register_output(
  "ftp", "org.embulk.output.ftp.FtpOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
