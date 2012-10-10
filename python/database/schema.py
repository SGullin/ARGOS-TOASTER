import sqlalchemy as sa

metadata = sa.MetaData()

# Define users table
sa.Table('users', metadata, \
        sa.Column('user_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('user_name', sa.String(64), nullable=False, \
                    unique=True), \
        sa.Column('real_name', sa.String(64), nullable=False, \
                    unique=True), \
        sa.Column('email_address', sa.String(64), nullable=False, \
                    unique=True), \
        sa.Column('passwd_hash', sa.String(64)), \
        sa.Column('active', sa.Boolean, nullable=False, \
                    default=True), \
        sa.Column('admin', sa.Boolean, nullable=False, \
                    default=False), \
        mysql_engine='InnoDB')

# Define versions table
sa.Table('versions', metadata, \
        sa.Column('version_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('pipeline_githash', sa.String(64), nullable=False), \
        sa.Column('psrchive_githash', sa.String(64), nullable=False), \
        sa.Column('tempo2_cvsrevno', sa.String(64), nullable=False), \
        sa.UniqueConstraint('pipeline_githash', 'psrchive_githash', \
                                'tempo2_cvsrevno'), \
        mysql_engine='InnoDB')

# Define toas table
sa.Table('toas', metadata, \
        sa.Column('toa_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('process_id', sa.Integer, \
                    sa.ForeignKey("process.process_id"), \
                    nullable=False), \
        sa.Column('template_id', sa.Integer, \
                    sa.ForeignKey("templates.template_id"), \
                    nullable=False), \
        sa.Column('rawfile_id', sa.Integer, \
                    sa.ForeignKey("rawfiles.rawfile_id"), \
                    nullable=False), \
        sa.Column('pulsar_id', sa.Integer, \
                    sa.ForeignKey("pulsars.pulsar_id"), \
                    nullable=False), \
        sa.Column('obssystem_id', sa.Integer, \
                    sa.ForeignKey("obssystems.obssystem_id"), \
                    nullable=False), \
        sa.Column('imjd', sa.Integer, nullable=False), \
        sa.Column('fmjd', sa.Float(53), nullable=False), \
        sa.Column('freq', sa.Float(24), nullable=False), \
        sa.Column('toa_unc_us', sa.Float(24), nullable=False), \
        mysql_engine='InnoDB')

# Define process table
sa.Table('process', metadata, \
        sa.Column('process_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('version_id', sa.Integer, \
                    sa.ForeignKey("versions.version_id"), \
                    nullable=False), \
        sa.Column('rawfile_id', sa.Integer, \
                    sa.ForeignKey("rawfiles.rawfile_id"), \
                    nullable=False), \
        sa.Column('template_id', sa.Integer, \
                    sa.ForeignKey("templates.template_id"), \
                    nullable=False), \
        sa.Column('parfile_id', sa.Integer, \
                    sa.ForeignKey("parfiles.parfile_id"), \
                    nullable=False), \
        sa.Column('user_id', sa.Integer, \
                    sa.ForeignKey("users.user_id"), \
                    nullable=False), \
        sa.Column('add_time', sa.DateTime, nullable=False), \
        sa.Column('input_args', sa.Text, nullable=False), \
        sa.Column('nchan', sa.Integer, nullable=False), \
        sa.Column('nsub', sa.Integer, nullable=False), \
        sa.Column('dm', sa.Float(53), nullable=False), \
        sa.Column('toa_fitting_method', sa.String(12), nullable=False), \
        mysql_engine='InnoDB')

# Define toa_diagnostics table
sa.Table('toa_diagnostics', metadata, \
        sa.Column('toa_diagnostic_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('toa_id', sa.Integer, \
                    sa.ForeignKey("toas.toa_id"), \
                    nullable=False), \
        sa.Column('value', sa.Float(53), nullable=False), \
        sa.Column('type', sa.String(16), nullable=False), \
        sa.UniqueConstraint('toa_id', 'type'), \
        mysql_engine='InnoDB')

# Define toa_diagnostic_plots table
sa.Table('toa_diagnostic_plots', metadata, \
        sa.Column('toa_diagnostic_plot_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('toa_id', sa.Integer, \
                    sa.ForeignKey("toas.toa_id"), \
                    nullable=False), \
        sa.Column('filename', sa.String(512), nullable=False, \
                    unique=True), \
        sa.Column('filepath', sa.String(512), nullable=False), \
        sa.Column('plot_type', sa.String(16), nullable=False), \
        sa.UniqueConstraint('toa_id', 'plot_type'), \
        mysql_engine='InnoDB')

# Define timfiles table
sa.Table('timfiles', metadata, \
        sa.Column('timfile_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('user_id', sa.Integer, \
                    sa.ForeignKey("users.user_id"), \
                    nullable=False), \
        sa.Column('comments', sa.Text, nullable=False), \
        sa.Column('create_time', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.Column('db_query', sa.Text, nullable=False), \
        mysql_engine='InnoDB')

# Define toa_tim table (mapping which TOAs are included in which .tim files)
sa.Table('toa_tim', metadata, \
        sa.Column('timfile_id', sa.Integer, \
                    sa.ForeignKey("timfiles.timfile_id"), \
                    nullable=False), \
        sa.Column('toa_id', sa.Integer, \
                    sa.ForeignKey("toas.toa_id"), \
                    nullable=False), \
        sa.UniqueConstraint('toa_id', 'timfile_id'), \
        mysql_engine='InnoDB')

# Define templates table
sa.Table('templates', metadata, \
        sa.Column('template_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('pulsar_id', sa.Integer, \
                    sa.ForeignKey("pulsars.pulsar_id"), \
                    nullable=False), \
        sa.Column('obssystem_id', sa.Integer, \
                    sa.ForeignKey("obssystems.obssystem_id"), \
                    nullable=False), \
        sa.Column('user_id', sa.Integer, \
                    sa.ForeignKey("users.user_id"), \
                    nullable=False), \
        sa.Column('nbin', sa.Integer, nullable=True), \
        sa.Column('is_analytic', sa.Boolean, nullable=False), \
        sa.Column('filepath', sa.String(512), nullable=False), \
        sa.Column('filename', sa.String(512), nullable=False, \
                    unique=True), \
        sa.Column('md5sum', sa.String(64), nullable=False, \
                    unique=True), \
        sa.Column('add_time', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.Column('comments', sa.Text, nullable=False), \
        sa.UniqueConstraint('pulsar_id', 'obssystem_id'), \
        mysql_engine='InnoDB')

# Define telescopes table
sa.Table('telescopes', metadata, \
        sa.Column('telescope_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('telescope_name', sa.String(64), nullable=False, \
                    unique=True), \
        sa.Column('latitude', sa.Float(53), nullable=True), \
        sa.Column('longitude', sa.Float(53), nullable=True), \
        sa.Column('datum', sa.String(64), nullable=True), \
        sa.Column('itrf_x', sa.Float(53), nullable=False), \
        sa.Column('itrf_y', sa.Float(53), nullable=False), \
        sa.Column('itrf_z', sa.Float(53), nullable=False), \
        sa.Column('telescope_abbrev', sa.String(16), nullable=False, \
                    unique=True), \
        sa.Column('telescope_code', sa.String(2), nullable=False, \
                    unique=True), \
        mysql_engine='InnoDB')

# Define telescope_aliases table
sa.Table('telescope_aliases', metadata, \
        sa.Column('telescope_alias_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('telescope_id', sa.Integer, \
                    sa.ForeignKey('telescopes.telescope_id'), \
                    nullable=False), \
        sa.Column('telescope_alias', sa.String(64), nullable=False, \
                    unique=True), \
        mysql_engine='InnoDB')

# Define rawfiles table
sa.Table('rawfiles', metadata, \
        sa.Column('rawfile_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('filename', sa.String(512), nullable=False, \
                    unique=True), \
        sa.Column('filepath', sa.String(512), nullable=False), \
        sa.Column('md5sum', sa.String(512), nullable=False, \
                    unique=True), \
        sa.Column('add_time', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.Column('user_id', sa.Integer, \
                    sa.ForeignKey('users.user_id'), \
                    nullable=False), \
        sa.Column('comments', sa.Text, nullable=True), \
        sa.Column('pulsar_id', sa.Integer, \
                    sa.ForeignKey('pulsars.pulsar_id'), \
                    nullable=False), \
        sa.Column('obssystem_id', sa.Integer, \
                    sa.ForeignKey('obssystems.obssystem_id'), \
                    nullable=False), \
        sa.Column('nbin', sa.Integer, nullable=True), \
        sa.Column('nchan', sa.Integer, nullable=True), \
        sa.Column('npol', sa.Integer, nullable=True), \
        sa.Column('nsub', sa.Integer, nullable=True), \
        sa.Column('type', sa.String(32), nullable=True), \
        sa.Column('site', sa.String(16), nullable=True), \
        sa.Column('name', sa.String(16), nullable=True), \
        sa.Column('datatype', sa.String(32), nullable=True), \
        sa.Column('coord', sa.String(32), nullable=True), \
        sa.Column('freq', sa.Float(24), nullable=True), \
        sa.Column('bw', sa.Float(24), nullable=True), \
        sa.Column('dm', sa.Float(24), nullable=True), \
        sa.Column('rm', sa.Float(24), nullable=True), \
        sa.Column('dmc', sa.Float(24), nullable=True), \
        sa.Column('rmc', sa.Float(24), nullable=True), \
        sa.Column('polc', sa.Float(24), nullable=True), \
        sa.Column('scale', sa.String(16), nullable=True), \
        sa.Column('state', sa.String(16), nullable=True), \
        sa.Column('length', sa.Float(23), nullable=True), \
        sa.Column('rcvr_name', sa.String(16), nullable=True), \
        sa.Column('rcvr_basis', sa.String(16), nullable=True), \
        sa.Column('be_name', sa.String(16), nullable=True), \
        mysql_engine='InnoDB')

# Define raw_diagnostics table
sa.Table('raw_diagnostics', metadata, \
        sa.Column('raw_diagnostic_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('rawfile_id', sa.Integer, \
                    sa.ForeignKey("rawfiles.rawfile_id"), \
                    nullable=False), \
        sa.Column('value', sa.Float(53), nullable=False), \
        sa.Column('type', sa.String(16), nullable=False), \
        sa.UniqueConstraint('rawfile_id', 'type'), \
        mysql_engine='InnoDB')

# Define raw_diagnostic_plots table
sa.Table('raw_diagnostic_plots', metadata, \
        sa.Column('raw_diagnostic_plot_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('rawfile_id', sa.Integer, \
                    sa.ForeignKey("rawfiles.rawfile_id"), \
                    nullable=False), \
        sa.Column('filename', sa.String(512), nullable=False, \
                    unique=True), \
        sa.Column('filepath', sa.String(512), nullable=False), \
        sa.Column('plot_type', sa.String(16), nullable=False), \
        sa.UniqueConstraint('rawfile_id', 'plot_type'), \
        mysql_engine='InnoDB')

# Define pulsars table
sa.Table('pulsars', metadata, \
        sa.Column('pulsar_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('pulsar_name', sa.String(20), nullable=False, \
                    unique=True), \
        mysql_engine='InnoDB')

# Define pulsar_aliases table
sa.Table('pulsar_aliases', metadata, \
        sa.Column('pulsar_alias_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('pulsar_id', sa.Integer, \
                    sa.ForeignKey('pulsars.pulsar_id'), \
                    nullable=False), \
        sa.Column('pulsar_alias', sa.String(20), nullable=False, \
                    unique=True), \
        mysql_engine='InnoDB')

# Define proc_diagnostics table
sa.Table('proc_diagnostics', metadata, \
        sa.Column('proc_diagnostic_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('process_id', sa.Integer, \
                    sa.ForeignKey("process.process_id"), \
                    nullable=False), \
        sa.Column('value', sa.Float(53), nullable=False), \
        sa.Column('type', sa.String(16), nullable=False), \
        sa.UniqueConstraint('process_id', 'type'), \
        mysql_engine='InnoDB')

# Define proc_diagnostic_plots table
sa.Table('proc_diagnostic_plots', metadata, \
        sa.Column('proc_diagnostic_plot_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('process_id', sa.Integer, \
                    sa.ForeignKey("process.process_id"), \
                    nullable=False), \
        sa.Column('filename', sa.String(512), nullable=False, \
                    unique=True), \
        sa.Column('filepath', sa.String(512), nullable=False), \
        sa.Column('plot_type', sa.String(16), nullable=False), \
        sa.UniqueConstraint('process_id', 'plot_type'), \
        mysql_engine='InnoDB')

# Define parfiles table
sa.Table('parfiles', metadata, \
        sa.Column('parfile_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('filename', sa.String(512), nullable=False, \
                    unique=True), \
        sa.Column('filepath', sa.String(512), nullable=False), \
        sa.Column('md5sum', sa.String(512), nullable=False, \
                    unique=True), \
        sa.Column('add_time', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.Column('user_id', sa.Integer, \
                    sa.ForeignKey('users.user_id'), \
                    nullable=False), \
        sa.Column('pulsar_id', sa.Integer, \
                    sa.ForeignKey('pulsars.pulsar_id'), \
                    nullable=False), \
        sa.Column('psrj', sa.String(12), nullable=True), \
        sa.Column('psrb', sa.String(12), nullable=True), \
        sa.Column('psr', sa.String(12), nullable=True), \
        sa.Column('raj', sa.String(32), nullable=True), \
        sa.Column('decj', sa.String(32), nullable=True), \
        sa.Column('pepoch', sa.Float(53), nullable=True), \
        sa.Column('f0', sa.Float(53), nullable=True), \
        sa.Column('f1', sa.Float(53), nullable=True), \
        sa.Column('dm', sa.Float(53), nullable=True), \
        sa.Column('pmra', sa.Float(53), nullable=True), \
        sa.Column('pmdec', sa.Float(53), nullable=True), \
        sa.Column('px', sa.Float(53), nullable=True), \
        sa.Column('dmepoch', sa.Float(53), nullable=True), \
        sa.Column('posepoch', sa.Float(53), nullable=True), \
        sa.Column('binary_model', sa.String(6), nullable=True), \
        sa.Column('pb', sa.Float(53), nullable=True), \
        sa.Column('ecc', sa.Float(53), nullable=True), \
        sa.Column('a1', sa.Float(53), nullable=True), \
        sa.Column('t0', sa.Float(53), nullable=True), \
        sa.Column('om', sa.Float(53), nullable=True), \
        sa.Column('tasc', sa.Float(53), nullable=True), \
        sa.Column('eps1', sa.Float(53), nullable=True), \
        sa.Column('eps2', sa.Float(53), nullable=True), \
        sa.Column('pbdot', sa.Float(53), nullable=True), \
        sa.Column('omdot', sa.Float(53), nullable=True), \
        sa.Column('a1dot', sa.Float(53), nullable=True), \
        sa.Column('sini', sa.Float(53), nullable=True), \
        sa.Column('m2', sa.Float(53), nullable=True), \
        sa.Column('start', sa.Float(53), nullable=True), \
        sa.Column('finish', sa.Float(53), nullable=True), \
        sa.Column('ephem', sa.String(8), nullable=True), \
        sa.Column('clk', sa.String(16), nullable=True), \
        sa.Column('tzrmjd', sa.Float(53), nullable=True), \
        sa.Column('tzrfrq', sa.Float(53), nullable=True), \
        sa.Column('tzrsite', sa.String(2), nullable=True), \
        mysql_engine='InnoDB')

# Create obssystem table
sa.Table('obssystems', metadata, \
        sa.Column('obssystem_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('name', sa.String(64), nullable=False, \
                    unique=True), \
        sa.Column('telescope_id', sa.Integer, \
                    sa.ForeignKey('telescopes.telescope_id'), \
                    nullable=False), \
        sa.Column('frontend', sa.String(64), nullable=False), \
        sa.Column('backend', sa.String(64), nullable=False), \
        sa.Column('clock', sa.String(64), nullable=False), \
        sa.UniqueConstraint('telescope_id', 'frontend', 'backend', \
                            'clock'), \
        mysql_engine='InnoDB')

# Create master_templates table
sa.Table('master_templates', metadata, \
        sa.Column('template_id', sa.Integer, \
                    sa.ForeignKey("templates.template_id"), \
                    nullable=False), \
        sa.Column('pulsar_id', sa.Integer, \
                    sa.ForeignKey('pulsars.pulsar_id'), \
                    nullable=False), \
        sa.Column('obssystem_id', sa.Integer, \
                    sa.ForeignKey("obssystems.obssystem_id"), \
                    nullable=False), \
        sa.UniqueConstraint('pulsar_id', 'obssystem_id'), \
        mysql_engine='InnoDB')

# Create master_parfiles table
sa.Table('master_parfiles', metadata, \
        sa.Column('parfile_id', sa.Integer, \
                    sa.ForeignKey("parfiles.parfile_id"), \
                    nullable=False), \
        sa.Column('pulsar_id', sa.Integer, \
                    sa.ForeignKey('pulsars.pulsar_id'), \
                    nullable=False, unique=True), \
        mysql_engine='InnoDB')
