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
        mysql_engine='InnoDB', mysql_charset='ascii')

# Define versions table
sa.Table('versions', metadata, \
        sa.Column('version_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('pipeline_githash', sa.String(64), nullable=False), \
        sa.Column('psrchive_githash', sa.String(64), nullable=False), \
        sa.Column('tempo2_cvsrevno', sa.String(64), nullable=False), \
        sa.UniqueConstraint('pipeline_githash', 'psrchive_githash', \
                                'tempo2_cvsrevno'), \
        mysql_engine='InnoDB', mysql_charset='ascii')

# Define toas table
sa.Table('toas', metadata, \
        sa.Column('toa_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('process_id', sa.Integer, \
                    sa.ForeignKey("process.process_id", name="fk_toas_proc"), \
                    nullable=False), \
        sa.Column('template_id', sa.Integer, \
                    sa.ForeignKey("templates.template_id", name="fk_toas_temp"), \
                    nullable=False), \
        sa.Column('rawfile_id', sa.Integer, \
                    sa.ForeignKey("rawfiles.rawfile_id", name="fk_toas_raw"), \
                    nullable=False), \
        sa.Column('pulsar_id', sa.Integer, \
                    sa.ForeignKey("pulsars.pulsar_id", name="fk_toas_psr"), \
                    nullable=False), \
        sa.Column('obssystem_id', sa.Integer, \
                    sa.ForeignKey("obssystems.obssystem_id", name="fk_toas_obssys"), \
                    nullable=False), \
        sa.Column('imjd', sa.Integer, nullable=False), \
        sa.Column('fmjd', sa.Float(53), nullable=False), \
        sa.Column('freq', sa.Float(24), nullable=False), \
        sa.Column('toa_unc_us', sa.Float(24), nullable=False), \
        sa.Column('bw', sa.Float(24), nullable=False), \
        sa.Column('length', sa.Float(24), nullable=False), \
        sa.Column('nbin', sa.Integer, nullable=False), \
        sa.Column('goodness_of_fit', sa.Float(24), nullable=True), \
        mysql_engine='InnoDB', mysql_charset='ascii')

# Define process table
sa.Table('process', metadata, \
        sa.Column('process_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('version_id', sa.Integer, \
                    sa.ForeignKey("versions.version_id", name="fk_proc_version"), \
                    nullable=False), \
        sa.Column('rawfile_id', sa.Integer, \
                    sa.ForeignKey("rawfiles.rawfile_id", name="fk_proc_raw"), \
                    nullable=False), \
        sa.Column('template_id', sa.Integer, \
                    sa.ForeignKey("templates.template_id", name="fk_proc_temp"), \
                    nullable=False), \
        sa.Column('parfile_id', sa.Integer, \
                    sa.ForeignKey("parfiles.parfile_id", name="fk_proc_par"), \
                    nullable=True), \
        sa.Column('user_id', sa.Integer, \
                    sa.ForeignKey("users.user_id", name="fk_proc_user"), \
                    nullable=False), \
        sa.Column('add_time', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.Column('manipulator', sa.String(32), nullable=False), \
        sa.Column('manipulator_args', sa.Text, nullable=False), \
        sa.Column('nchan', sa.Integer, nullable=False), \
        sa.Column('nsub', sa.Integer, nullable=False), \
        sa.Column('toa_fitting_method', sa.String(12), nullable=False), \
        mysql_engine='InnoDB', mysql_charset='ascii')

# Define toa_diagnostics table
sa.Table('toa_diagnostics', metadata, \
        sa.Column('toa_diagnostic_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('toa_id', sa.Integer, \
                    sa.ForeignKey("toas.toa_id", name="fk_toadig_toa"), \
                    nullable=False), \
        sa.Column('value', sa.Float(53), nullable=False), \
        sa.Column('type', sa.String(16), nullable=False), \
        sa.UniqueConstraint('toa_id', 'type'), \
        mysql_engine='InnoDB', mysql_charset='ascii')

# Define toa_diagnostic_plots table
sa.Table('toa_diagnostic_plots', metadata, \
        sa.Column('toa_diagnostic_plot_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('toa_id', sa.Integer, \
                    sa.ForeignKey("toas.toa_id", name="fk_toadiagplot_toa"), \
                    nullable=False), \
        sa.Column('filename', sa.String(256), nullable=False, \
                    unique=True), \
        sa.Column('filepath', sa.String(512), nullable=False), \
        sa.Column('plot_type', sa.String(16), nullable=False), \
        sa.UniqueConstraint('toa_id', 'plot_type'), \
        mysql_engine='InnoDB', mysql_charset='ascii')

# Define timfiles table
sa.Table('timfiles', metadata, \
        sa.Column('timfile_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('user_id', sa.Integer, \
                    sa.ForeignKey("users.user_id", name="fk_tim_user"), \
                    nullable=False), \
        sa.Column('comments', sa.Text, nullable=False), \
        sa.Column('add_time', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.Column('version_id', sa.Integer, \
                    sa.ForeignKey("versions.version_id", name="fk_tim_version"), \
                    nullable=False), \
        sa.Column('pulsar_id', sa.Integer, \
                    sa.ForeignKey("pulsars.pulsar_id", name="fk_tim_psr"), \
                    nullable=False), \
        sa.Column('input_args', sa.Text, nullable=False), \
        mysql_engine='InnoDB', mysql_charset='ascii')

# Define toa_tim table (mapping which TOAs are included in which .tim files)
sa.Table('toa_tim', metadata, \
        sa.Column('timfile_id', sa.Integer, \
                    sa.ForeignKey("timfiles.timfile_id", name="fk_toatim_tim"), \
                    nullable=False), \
        sa.Column('toa_id', sa.Integer, \
                    sa.ForeignKey("toas.toa_id", name="fk_toatim_toa"), \
                    nullable=False), \
        sa.UniqueConstraint('toa_id', 'timfile_id'), \
        mysql_engine='InnoDB', mysql_charset='ascii')

# Define templates table
sa.Table('templates', metadata, \
        sa.Column('template_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('pulsar_id', sa.Integer, \
                    sa.ForeignKey("pulsars.pulsar_id", name="fk_temp_psr"), \
                    nullable=False), \
        sa.Column('obssystem_id', sa.Integer, \
                    sa.ForeignKey("obssystems.obssystem_id", name="fk_temp_obssys"), \
                    nullable=False), \
        sa.Column('user_id', sa.Integer, \
                    sa.ForeignKey("users.user_id", name="fk_temp_user"), \
                    nullable=False), \
        sa.Column('nbin', sa.Integer, nullable=True), \
        sa.Column('filepath', sa.String(512), nullable=False), \
        sa.Column('filename', sa.String(256), nullable=False, \
                    unique=True), \
        sa.Column('md5sum', sa.String(64), nullable=False, \
                    unique=True), \
        sa.Column('add_time', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.Column('comments', sa.Text, nullable=False), \
        mysql_engine='InnoDB', mysql_charset='ascii')

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
        mysql_engine='InnoDB', mysql_charset='ascii')

# Define telescope_aliases table
sa.Table('telescope_aliases', metadata, \
        sa.Column('telescope_alias_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('telescope_id', sa.Integer, \
                    sa.ForeignKey('telescopes.telescope_id', name="fk_telalias_tel"), \
                    nullable=False), \
        sa.Column('telescope_alias', sa.String(64), nullable=False, \
                    unique=True), \
        mysql_engine='InnoDB', mysql_charset='ascii')

# Define rawfiles table
sa.Table('rawfiles', metadata, \
        sa.Column('rawfile_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('filename', sa.String(256), nullable=False, \
                    unique=True), \
        sa.Column('filepath', sa.String(512), nullable=False), \
        sa.Column('filesize', sa.Integer, nullable=False), \
        sa.Column('md5sum', sa.String(64), nullable=False, \
                    unique=True), \
        sa.Column('add_time', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.Column('user_id', sa.Integer, \
                    sa.ForeignKey('users.user_id', name="fk_raw_user"), \
                    nullable=False), \
        sa.Column('comments', sa.Text, nullable=True), \
        sa.Column('pulsar_id', sa.Integer, \
                    sa.ForeignKey('pulsars.pulsar_id', name="fk_raw_psr"), \
                    nullable=False), \
        sa.Column('obssystem_id', sa.Integer, \
                    sa.ForeignKey('obssystems.obssystem_id', name="fk_raw_obssys"), \
                    nullable=False), \
        sa.Column('nbin', sa.Integer, nullable=True), \
        sa.Column('nchan', sa.Integer, nullable=True), \
        sa.Column('npol', sa.Integer, nullable=True), \
        sa.Column('nsub', sa.Integer, nullable=True), \
        sa.Column('type', sa.String(32), nullable=True), \
        sa.Column('telescop', sa.String(16), nullable=True), \
        sa.Column('name', sa.String(16), nullable=True), \
        sa.Column('datatype', sa.String(32), nullable=True), \
        sa.Column('coord', sa.String(32), nullable=True), \
        sa.Column('freq', sa.Float(24), nullable=True), \
        sa.Column('bw', sa.Float(24), nullable=True), \
        sa.Column('dm', sa.Float(24), nullable=True), \
        sa.Column('rm', sa.Float(24), nullable=True), \
        sa.Column('dmc', sa.Float(24), nullable=True), \
        sa.Column('rm_c', sa.Float(24), nullable=True), \
        sa.Column('pol_c', sa.Float(24), nullable=True), \
        sa.Column('scale', sa.String(16), nullable=True), \
        sa.Column('state', sa.String(16), nullable=True), \
        sa.Column('length', sa.Float(24), nullable=True), \
        sa.Column('mjd', sa.Float(53), nullable=True), \
        sa.Column('rcvr', sa.String(16), nullable=True), \
        sa.Column('basis', sa.String(16), nullable=True), \
        sa.Column('backend', sa.String(16), nullable=True), \
        mysql_engine='InnoDB', mysql_charset='ascii')

# Definte replacement_rawfiles
sa.Table('replacement_rawfiles', metadata, \
        sa.Column('obsolete_rawfile_id', sa.Integer, \
                    sa.ForeignKey("rawfiles.rawfile_id", name="fk_obsolete"), \
                    nullable=False, unique=True), \
        sa.Column('replacement_rawfile_id', sa.Integer, \
                    sa.ForeignKey("rawfiles.rawfile_id", name="fk_replacement"), \
                    nullable=False), \
        sa.Column('add_time', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.Column('user_id', sa.Integer, \
                    sa.ForeignKey('users.user_id', name="fk_raw_user"), \
                    nullable=False), \
        sa.Column('comments', sa.Text, nullable=False), \
        mysql_engine='InnoDB', mysql_charset='ascii')

# Define raw_diagnostics table
sa.Table('raw_diagnostics', metadata, \
        sa.Column('raw_diagnostic_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('rawfile_id', sa.Integer, \
                    sa.ForeignKey("rawfiles.rawfile_id", name="fk_rawdiag_raw"), \
                    nullable=False), \
        sa.Column('value', sa.Float(53), nullable=False), \
        sa.Column('type', sa.String(16), nullable=False), \
        sa.UniqueConstraint('rawfile_id', 'type'), \
        mysql_engine='InnoDB', mysql_charset='ascii')

# Define raw_diagnostic_plots table
sa.Table('raw_diagnostic_plots', metadata, \
        sa.Column('raw_diagnostic_plot_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('rawfile_id', sa.Integer, \
                    sa.ForeignKey("rawfiles.rawfile_id", name="fk_rawdiagplot_raw"), \
                    nullable=False), \
        sa.Column('filename', sa.String(256), nullable=False, \
                    unique=True), \
        sa.Column('filepath', sa.String(512), nullable=False), \
        sa.Column('plot_type', sa.String(16), nullable=False), \
        sa.UniqueConstraint('rawfile_id', 'plot_type'), \
        mysql_engine='InnoDB', mysql_charset='ascii')

# Define curators table
sa.Table('curators', metadata, \
        sa.Column('pulsar_id', sa.Integer, \
                    sa.ForeignKey('pulsars.pulsar_id', name="fk_cura_psr"), \
                    nullable=False), \
        sa.Column('user_id', sa.Integer, \
                    sa.ForeignKey('users.user_id', name="fk_cura_user"), \
                    nullable=True), \
        sa.UniqueConstraint('pulsar_id', 'user_id'), \
        mysql_engine='InnoDB', mysql_charset='ascii')

# Define pulsars table
sa.Table('pulsars', metadata, \
        sa.Column('pulsar_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('pulsar_name', sa.String(20), nullable=False, \
                    unique=True), \
        mysql_engine='InnoDB', mysql_charset='ascii')

# Define pulsar_aliases table
sa.Table('pulsar_aliases', metadata, \
        sa.Column('pulsar_alias_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('pulsar_id', sa.Integer, \
                    sa.ForeignKey('pulsars.pulsar_id', name="fk_psralias_psr"), \
                    nullable=False), \
        sa.Column('pulsar_alias', sa.String(20), nullable=False, \
                    unique=True), \
        mysql_engine='InnoDB', mysql_charset='ascii')

# Define proc_diagnostics table
sa.Table('proc_diagnostics', metadata, \
        sa.Column('proc_diagnostic_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('process_id', sa.Integer, \
                    sa.ForeignKey("process.process_id", name="fk_procdiag_proc"), \
                    nullable=False), \
        sa.Column('value', sa.Float(53), nullable=False), \
        sa.Column('type', sa.String(16), nullable=False), \
        sa.UniqueConstraint('process_id', 'type'), \
        mysql_engine='InnoDB', mysql_charset='ascii')

# Define proc_diagnostic_plots table
sa.Table('proc_diagnostic_plots', metadata, \
        sa.Column('proc_diagnostic_plot_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('process_id', sa.Integer, \
                    sa.ForeignKey("process.process_id", name="fk_procdiagplot_proc"), \
                    nullable=False), \
        sa.Column('filename', sa.String(256), nullable=False, \
                    unique=True), \
        sa.Column('filepath', sa.String(512), nullable=False), \
        sa.Column('plot_type', sa.String(16), nullable=False), \
        sa.UniqueConstraint('process_id', 'plot_type'), \
        mysql_engine='InnoDB', mysql_charset='ascii')

# Define parfiles table
sa.Table('parfiles', metadata, \
        sa.Column('parfile_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('filename', sa.String(256), nullable=False, \
                    unique=True), \
        sa.Column('filepath', sa.String(512), nullable=False), \
        sa.Column('md5sum', sa.String(64), nullable=False, \
                    unique=True), \
        sa.Column('add_time', sa.DateTime, nullable=False, \
                    default=sa.func.now()), \
        sa.Column('user_id', sa.Integer, \
                    sa.ForeignKey('users.user_id', name="fk_par_user"), \
                    nullable=False), \
        sa.Column('pulsar_id', sa.Integer, \
                    sa.ForeignKey('pulsars.pulsar_id', name="fk_par_psr"), \
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
        mysql_engine='InnoDB', mysql_charset='ascii')

# Create obssystem table
sa.Table('obssystems', metadata, \
        sa.Column('obssystem_id', sa.Integer, primary_key=True, \
                    autoincrement=True, nullable=False), \
        sa.Column('name', sa.String(64), nullable=False, \
                    unique=True), \
        sa.Column('telescope_id', sa.Integer, \
                    sa.ForeignKey('telescopes.telescope_id', name="fk_obssys_tel"), \
                    nullable=False), \
        sa.Column('frontend', sa.String(64), nullable=False), \
        sa.Column('backend', sa.String(64), nullable=False), \
        sa.Column('band_descriptor', sa.String(16), nullable=True), \
        sa.Column('clock', sa.String(64), nullable=False), \
        sa.UniqueConstraint('telescope_id', 'frontend', 'backend', \
                            'clock'), \
        mysql_engine='InnoDB', mysql_charset='ascii')

# Create master_templates table
sa.Table('master_templates', metadata, \
        sa.Column('template_id', sa.Integer, \
                    sa.ForeignKey("templates.template_id", name="fk_mt_temp"), \
                    nullable=False), \
        sa.Column('pulsar_id', sa.Integer, \
                    sa.ForeignKey('pulsars.pulsar_id', name="fk_mt_psr"), \
                    nullable=False), \
        sa.Column('obssystem_id', sa.Integer, \
                    sa.ForeignKey("obssystems.obssystem_id", name="fk_mt_obssys"), \
                    nullable=False), \
        sa.UniqueConstraint('pulsar_id', 'obssystem_id'), \
        mysql_engine='InnoDB', mysql_charset='ascii')

# Create master_parfiles table
sa.Table('master_parfiles', metadata, \
        sa.Column('parfile_id', sa.Integer, \
                    sa.ForeignKey("parfiles.parfile_id", name="fk_mp_par"), \
                    nullable=False), \
        sa.Column('pulsar_id', sa.Integer, \
                    sa.ForeignKey('pulsars.pulsar_id', name="fk_mp_psr"), \
                    nullable=False, unique=True), \
        mysql_engine='InnoDB', mysql_charset='ascii')
