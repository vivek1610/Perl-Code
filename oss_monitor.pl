#!/usr/bin/perl -w
use strict;
use Env;

# Validating parameter exist or not
my $_m_number;
if (defined($ARGV[0])) {
        $_m_number = $ARGV[0];
        chomp ($_m_number);
} else { die "Please provide the monitor number along with the script './<SCRIPT>.pl 8004AQ'\n"; }

# Environment details
$ENV{'ORACLE_HOME'} = `/home/ossmon/ALERTS/repository/environment_variabls.pl ORACLE_HOME`;
$ENV{'PATH'} = `/home/ossmon/ALERTS/repository/environment_variabls.pl PATH`;
$ENV{'TNS_ADMIN'} = `/home/ossmon/ALERTS/repository/environment_variabls.pl TNS_ADMIN`;
$ENV{'LIBPATH'} = `/home/ossmon/ALERTS/repository/environment_variabls.pl LIBPATH`;

#Database credential info
my $credential_file="/home/ossmon/ALERTS/repository/ora_credential.csv";

my $_temp_dir = "/home/ossmon/ALERTS/temp"; `mkdir -p $_temp_dir`;
my $_sql_dir = "/home/ossmon/ALERTS/sql"; `mkdir -p $_sql_dir`;
my $_mail_out_dir = "/home/ossmon/ALERTS/outbox"; `mkdir -p $_mail_out_dir`;
my $_sent_dir = "/home/ossmon/ALERTS/sent"; `mkdir -p $_sent_dir`;

#Datails will be stored to this file, when select query to repository db and for monitor select query
my $_monitor_details_file = "$_temp_dir/$_m_number"."_details.sql";
my $_monitor_run_status_upd_file = "$_temp_dir/$_m_number"."_update_run_status.sql";
my $_noc_details_file = "$_temp_dir/$_m_number"."_noc_details.out";
my $_monitor_query_file = "$_sql_dir/$_m_number"."_monitor_query.sql";
my $_mail_out_file = "$_mail_out_dir/$_m_number"."_mail_out.html";
my $_upd_query_file = "$_temp_dir/$_m_number"."_UPD_details.sql";
my $_reflow_query_file = "$_sql_dir/$_m_number"."_UPD_monitor_query.sql";
my $_reflow_query_file_tmp = "$_sql_dir/$_m_number"."_UPD_monitor_query_tmp.sql";

# Pulling and Validating the repository DB credential
my $_repo_db_info = `cat $credential_file | grep -v ^# | grep 'REPO,'`; chomp($_repo_db_info);
die "Repository db info missing\n" if (!defined($_repo_db_info));

# Extracting the repository db user, password and sid details
my @_repo_db = split /,/ , $_repo_db_info; 

my $_history = "/home/ossmon/ALERTS/history"; `mkdir -p $_history`;
my $_track_file_name = "$_history/$_m_number-"."track_file"; chomp($_track_file_name); $_track_file_name .= ".txt";
my $_track_history_update = "$_sql_dir/$_m_number-"."track_file"; chomp($_track_history_update); $_track_history_update .= ".sql";
my $_sno_history_sql = "$_sql_dir/sno_history_sequence_pull.sql"; chomp($_sno_history_sql);

open EVENT_TARCK, ">> $_track_file_name" or die "Path not found '$_history'";
open UPDATE_TRACK, ">> $_track_history_update" or die "Path not found '$_history'";
open SNO_SEQ_PULL, ">> $_sno_history_sql" or die "Path not found '$_sql_dir'"; `>> $_sno_history_sql`;

my $_seq_file_exist = `grep -c "MONITOR_HST_SEQ.nextval" $_sno_history_sql`; chomp($_seq_file_exist);
if ( $_seq_file_exist < 1 ) { 
        print SNO_SEQ_PULL "set heading off\n";
        print SNO_SEQ_PULL "set linesize 10000\n";
        print SNO_SEQ_PULL "SET LONG 50000\n";
        print SNO_SEQ_PULL "SET LONGC 50000\n";
        print SNO_SEQ_PULL "select MONITOR_HST_SEQ.nextval from dual;\n";
        print SNO_SEQ_PULL "exit;\n";
        close SNO_SEQ_PULL;
}
my $_sno_seq_for_history = `sqlplus -s $_repo_db[1]/$_repo_db[2]\@$_repo_db[3] \@$_sno_history_sql | grep . | xargs`; chomp($_sno_seq_for_history);
my $_time = `date +"%m/%d/%Y %T"`; chomp($_time);
print EVENT_TARCK "[ $_time ] : TOOL EXECUTION STARTED\n";
print UPDATE_TRACK "INSERT INTO MONITOR_HISTORY (SNO,M_NUMBER,HISTORY_FILE) VALUES( $_sno_seq_for_history, '$_m_number', '$_track_file_name');\n";
print UPDATE_TRACK "COMMIT;\n";
print UPDATE_TRACK "EXIT;\n";
`sqlplus -s $_repo_db[1]/$_repo_db[2]\@$_repo_db[3] \@$_track_history_update > /dev/null`;

open GRAB_MONITOR_QUERY, ">> $_monitor_details_file " or die "Path not found '$_temp_dir'";
open UPD_MONITOR_STATUS, ">> $_monitor_run_status_upd_file " or die "Path not found '$_temp_dir'";
open MONITOR_QUERY, ">> $_monitor_query_file " or die "Path not found '$_sql_dir'";
open MAILOUT, ">> $_mail_out_file " or die "Path not found '$_mail_out_dir'"; `> $_mail_out_file`;
open NOC_NOTIFY, ">> $_noc_details_file " or die "Path not found '$_temp_dir'"; `>> $_noc_details_file`;
open UPD_MONITOR, ">> $_upd_query_file " or die "Path not found '$_temp_dir'"; 
open REFLOW_MONITOR, ">> $_reflow_query_file " or die "Path not found '$_sql_dir'"; 
open REFLOW_MONITOR_TMP, ">> $_reflow_query_file_tmp " or die "Path not found '$_sql_dir'";

# Grab the monitor details from repository database

# preparing the select query for the repository db to pull the monitor info
`> $_monitor_details_file`;
`> $_monitor_run_status_upd_file`;

$_time = `date +"%m/%d/%Y %T"`; chomp($_time);
print EVENT_TARCK "[ $_time ] : PREPARING SQL QUERY FILE ($_monitor_details_file) FOR GETTING THE MONITOR DETAILS FROM REPOSITORY DATABASE [$_repo_db[3]]\n";

print GRAB_MONITOR_QUERY "set heading off\n";
print GRAB_MONITOR_QUERY "set linesize 10000\n";
print GRAB_MONITOR_QUERY "SET LONG 50000\n";
print GRAB_MONITOR_QUERY "SET LONGC 50000\n";
print GRAB_MONITOR_QUERY "SELECT M_NUMBER||'<D>'||M_DB||'<D>'||M_SUBJECT||'<D>'||M_BODY||'<D>'||M_OWNER||'<D>'||M_SQL_QUERY||'<D>'||M_HELP_QUERY||'<D>'||M_COLUMNS||'<D>'||MAIL_TO||'<D>'||MAIL_CC||'<D>'||THRESHOLD||'<D>'||NOC_NOTIFY||'<D>'||NOC_SEVERITY||'<D>'||H_THRESHOLD||'<D>'||H_NOC_NOTIFY||'<D>'||H_NOC_SEVERITY||'<D>'||NOC_ALERT_NUMBER||'<D>'||KEY_COLUMNS||'<D>'||WIDTH||'<D>'||EML_BELOW_THOLD||'<D>'||M_ENV||'<D>'||IF_COUNT_ZERO||'<D>'||CONFLUENCE||'<D>'||UPD_ENABLE FROM OSS_MONITORS WHERE M_NUMBER = '$_m_number' AND DISABLED = 0;\n";
print GRAB_MONITOR_QUERY "exit;\n";
close GRAB_MONITOR_QUERY;

$_time = `date +"%m/%d/%Y %T"`; chomp($_time);
print EVENT_TARCK "[ $_time ] : EXECUTING THE SQL QUERY FILE ($_monitor_details_file) TO REPOSITORY DATABASE [$_repo_db[3]]\n";

# Executing pull monitor details query and storring details
my $_monitor_details_query_out = `sqlplus -s $_repo_db[1]/$_repo_db[2]\@$_repo_db[3] \@$_monitor_details_file | grep .`; chomp($_monitor_details_query_out);

# Validating if the monitor exist or not
if ( "$_monitor_details_query_out" =~ "no rows selected" ) {
        `rm -fr $_monitor_details_file`;
        $_time = `date +"%m/%d/%Y %T"`; chomp($_time);
        print EVENT_TARCK "[ $_time ] : FOUND THE MONITOR IS INVALID. TOOL IS STOPPED\n";
        `> $_track_history_update`;
        print UPDATE_TRACK "UPDATE MONITOR_HISTORY SET COMPLETE_DT = SYSDATE, STATUS = 'INVALID' WHERE SNO = $_sno_seq_for_history;\n";
        print UPDATE_TRACK "COMMIT;\n";
        print UPDATE_TRACK "EXIT;\n";
        `sqlplus -s $_repo_db[1]/$_repo_db[2]\@$_repo_db[3] \@$_track_history_update > /dev/null`;
        `rm -fr $_track_history_update`;
        die "'$_m_number' Monitor not exist";
}

$_time = `date +"%m/%d/%Y %T"`; chomp($_time);
print EVENT_TARCK "[ $_time ] : FOUND THE MONITOR IS VALID\n";
`> $_track_history_update`;
print UPDATE_TRACK "UPDATE MONITOR_HISTORY SET STATUS = 'INITIALIZING' WHERE SNO = $_sno_seq_for_history;\n";
print UPDATE_TRACK "COMMIT;\n";
print UPDATE_TRACK "EXIT;\n";
`sqlplus -s $_repo_db[1]/$_repo_db[2]\@$_repo_db[3] \@$_track_history_update > /dev/null`;

#print "$_monitor_details_query_out\n";
my @_monitor_details = split /<D>/, $_monitor_details_query_out;

$_time = `date +"%m/%d/%Y %T"`; chomp($_time);
print EVENT_TARCK "[ $_time ] : PREPARING THE MONITOR DETAILS\n";

# Assign all the required values 
my $_m_db               = $_monitor_details[1]; chomp($_m_db);
my $_m_owner            = $_monitor_details[4]; chomp($_m_owner);
my $_m_env              = $_monitor_details[20]; chomp($_m_env);
my $_m_subject          = "(Monitor: $_m_number $_m_owner) $_monitor_details[2] ["."$_m_env"."]"; chomp($_m_subject);
my $_m_body             = $_monitor_details[3]; chomp($_m_body);
my $_m_sql_query        = $_monitor_details[5]; chomp($_m_sql_query);
my $_m_help_query       = $_monitor_details[6]; chomp($_m_help_query);
my @_m_columns          = split /,/, $_monitor_details[7];
my $_mail_to            = $_monitor_details[8]; chomp($_mail_to);
my $_mail_cc            = $_monitor_details[9]; chomp($_mail_cc);
my $_threshold          = $_monitor_details[10]; chomp($_threshold);
my $_noc_notify         = $_monitor_details[11]; chomp($_noc_notify);
my $_noc_severity       = $_monitor_details[12]; chomp($_noc_severity);
my $_h_threshold        = $_monitor_details[13]; chomp($_h_threshold);
my $_h_noc_notify       = $_monitor_details[14]; chomp($_h_noc_notify);
my $_h_noc_severity     = $_monitor_details[15]; chomp($_h_noc_severity);
my $_noc_alert_number   = $_monitor_details[16]; chomp($_noc_alert_number);
my @_key_columns        = split /\|/, $_monitor_details[17];
my $_width              = $_monitor_details[18]; chomp($_width);
my $_eml_below_thold    = $_monitor_details[19]; chomp($_eml_below_thold);
my $_if_count_zero      = $_monitor_details[21]; chomp($_if_count_zero);
my $_upd_enable         = $_monitor_details[23]; chomp($_upd_enable);

my $_confluence = "NULL";
if (defined($_monitor_details[22])) {
        $_confluence    = $_monitor_details[22]; chomp($_confluence);
        $_time = `date +"%m/%d/%Y %T"`; chomp($_time);
        print EVENT_TARCK "[ $_time ] : CONFLUENCE LINK FOUND\n";
}
####################### Host value according to Alert Number ######################
=head
my $_m_number_hash  = {
						'Alert_1'=> 'stosippdb1.va.neustar.com',
						'Alert_2'=> 'stosippdb1.va.neustar.com',
						'Alert_3'=> 'stosippdb1.va.neustar.com',
						'Alert_4'=> 'stosippdb1.va.neustar.com',
						'Alert_5'=> 'stosippdb1.va.neustar.com',
						'Alert_6'=> 'stosippdb1.va.neustar.com',
						'Alert_7'=> 'stosippdb1.va.neustar.com',
						'Alert_8'=> 'stosippdb1.va.neustar.com',
						'Alert_9'=> 'stosippdb1.va.neustar.com'
					};
my $_hostname           = $_m_number_hash->{$_m_number};
=cut
####################################################################################

my $_hostname           = 'oms-message-queue';


# preparing the monitor query 
`> $_monitor_query_file`;
print MONITOR_QUERY "set heading off\n";
print MONITOR_QUERY "set linesize 10000\n";
print MONITOR_QUERY "SET LONG 50000\n";
print MONITOR_QUERY "SET LONGC 50000\n";
print MONITOR_QUERY "$_m_sql_query\n";
print MONITOR_QUERY "exit;\n";
close MONITOR_QUERY;


my $_db_info = `cat $credential_file | grep -v ^# | grep '$_m_db,'`; chomp($_db_info);

if ("$_db_info" !~ "$_m_db") { 
        $_time = `date +"%m/%d/%Y %T"`; chomp($_time);
        print EVENT_TARCK "[ $_time ] : MONITOR DATABASE [$_m_db] INFO IS INVALID. PLEASE ADD TO '$credential_file'. TOOL IS STOPPED\n";
        `> $_track_history_update`;
        print UPDATE_TRACK "UPDATE MONITOR_HISTORY SET COMPLETE_DT = SYSDATE, STATUS = 'INVALID_MDB' WHERE SNO = $_sno_seq_for_history;\n";
        print UPDATE_TRACK "COMMIT;\n";
        print UPDATE_TRACK "EXIT;\n";
        `sqlplus -s $_repo_db[1]/$_repo_db[2]\@$_repo_db[3] \@$_track_history_update > /dev/null`;
        `rm -fr $_track_history_update`;
        die "Db info missing\n";
}

$_time = `date +"%m/%d/%Y %T"`; chomp($_time);
print EVENT_TARCK "[ $_time ] : MONITOR DATABASE [$_m_db] INFO IS VALID\n";

my @_db = split /,/ , $_db_info; 


$_time = `date +"%m/%d/%Y %T"`; chomp($_time);
print EVENT_TARCK "[ $_time ] : EXECUTING THE MONITOR QUERY AGAINST THE DESTINATION DATABASE [$_db[3]]\n";
`> $_track_history_update`;
print UPDATE_TRACK "UPDATE MONITOR_HISTORY SET SQL_EXEC_DT = SYSDATE, STATUS = 'EXECUTING' WHERE SNO = $_sno_seq_for_history;\n";
print UPDATE_TRACK "COMMIT;\n";
print UPDATE_TRACK "EXIT;\n";
`sqlplus -s $_repo_db[1]/$_repo_db[2]\@$_repo_db[3] \@$_track_history_update > /dev/null`;

my $_query_out = `sqlplus -s $_db[1]/$_db[2]\@$_db[3] \@$_monitor_query_file | grep .`; chomp($_query_out);

$_time = `date +"%m/%d/%Y %T"`; chomp($_time);
print EVENT_TARCK "[ $_time ] : EXECUTED THE MONITOR QUERY AGAINST THE DESTINATION DATABASE [$_db[3]] AND THE DETAILS ARE BELOW\n";
print EVENT_TARCK "$_query_out\n";

`> $_track_history_update`;
print UPDATE_TRACK "UPDATE MONITOR_HISTORY SET SQL_EXE_COMP_DT = SYSDATE, STATUS = 'EXECUTED' WHERE SNO = $_sno_seq_for_history;\n";
print UPDATE_TRACK "COMMIT;\n";
print UPDATE_TRACK "EXIT;\n";
`sqlplus -s $_repo_db[1]/$_repo_db[2]\@$_repo_db[3] \@$_track_history_update > /dev/null`;


$_query_out =~ s/\n/<ENTER>/g;
my @_query_output = split /\<ENTER\>/, $_query_out;

my $htmlstart = qq#<table style='width:$_width\%' summary='Script output'>#;
my $trstart = qq#<tr>#;
my $trend = qq#</tr>#;
my $mid_cel_start = qq#<td scope="col" align='center'>#;
my $cel_start = qq#<td>#;
my $cel_end = qq#</td>#;
my $thr_cel_start = qq#<td  scope="col" bgcolor = '\#FFFF00' align='center'><font color="black"><B>#;
my $thr_cel_end = qq#</B></font></td>#;
my $h_thr_cel_start = qq#<td  scope="col" bgcolor = '\#FF0000' align='center'><font color="white"><B>#;
my $h_thr_cel_end = qq#</B></font></td>#;
my $column_start = qq#<th scope="col" bgcolor = '\#007700' align='left'><font color="white">#;
my $column_end = qq#</font></th>#;
my $htmlend = qq#</font>\n</table></p><br>#;

# Output prepare 

print MAILOUT qq#<style>#;
print MAILOUT qq#p{font-family:"Calibri";font-size:15px;line-height: 1.0;}#;
print MAILOUT qq#table{font-family:"Calibri";border-collapse:collapse; background-color:\#F3FEBD;}#;
print MAILOUT qq#th,tr,td{font-family:"Calibri";border: 1px solid black; font-size:15px; vertical-align:middle;}#;
print MAILOUT qq#</style>#;

#print MAILOUT qq#<a style="float: right; href="http://www.neustar.biz/"><img src="http://www.neustar.biz/website/static/img/logo.png"/></a>#;

print MAILOUT qq#<p>${_m_body}\n#;
        
my($_upd_executed, $_include_criteria, $_tmp_update_value, $_exclude_criteria, $_exclude_match, $_upd_key_tmp, $_key_criteria, $_table_name, $_default_upd_set, $_where_criteria, $_exclude_values, $_exclude_upd_enable, $_exclude_upd_set, $_upd_criteria, $_key_column, $_monitor_update_query_out, $_cnt, $_nk, $_ck, $_result_temp, $_tmp_loop, $_hnoc_notify_temp, $_noc_notify_temp, $status, $_exist, $_cel_cnt, $_skip_status, $_reflow_query_output_tmp);
my (@_exclude_tmp, @_monitor_update_query, @_exclude_key_values, @_upd_key_columns, @_query_result, @_notify_noc);
$_nk = "";
$_ck = "";
$_exist = "YES";
$status = 0;
$_key_column = 0;
$_upd_executed = 0;
my @_noc_key_column = split /,/, $_key_columns[0];
my @_count_key_column = split /,/, $_key_columns[1];
my $_count_keys = @_count_key_column; chomp($_count_keys);
if ($_upd_enable =~ "Y") {
        $_time = `date +"%m/%d/%Y %T"`; chomp($_time);
        print EVENT_TARCK "[ $_time ] : FOUND THE UPDATE FLAG IS SET TO TRUE. SO PREPARING THE QUERY TO PULL THE DETAILS FROM OSS_UPD_MONITOR [$_m_number]\n";
        `> $_reflow_query_file_tmp`;
        `> $_reflow_query_file`;
        `> $_upd_query_file`;
        print UPD_MONITOR "set heading off\n";
        print UPD_MONITOR "set linesize 10000\n";
        print UPD_MONITOR "SET LONG 50000\n";
        print UPD_MONITOR "SET LONGC 50000\n";
        print UPD_MONITOR "SELECT TABLE_NAME||'<D>'||DEFAULT_UPD_SET||'<D>'||WHERE_CRITERIA||'<D>'||KEY_COLUMN||'<D>'||EXCLUDE_KEY_VALUES||'<D>'||EXCLUDE_UPD_ENABLE||'<D>'||EXCLUDE_UPD_SET||'<D>'||UPD_CRITERIA||'<D>'||INCLUDE_CRITERIA||'<D>'||EXCLUDE_CRITERIA FROM OSS_UPD_MONITOR WHERE M_NUMBER = '$_m_number';\n";
        print UPD_MONITOR "exit;\n";
        close UPD_MONITOR;

        # Executing update monitor details query and storring same
        $_monitor_update_query_out = `sqlplus -s $_repo_db[1]/$_repo_db[2]\@$_repo_db[3] \@$_upd_query_file | grep .`; chomp($_monitor_update_query_out);
        @_monitor_update_query = split /<D>/, $_monitor_update_query_out;

        $_table_name            = $_monitor_update_query[0]; chomp($_table_name);
        $_default_upd_set       = $_monitor_update_query[1]; chomp($_default_upd_set);
        $_where_criteria        = $_monitor_update_query[2]; chomp($_where_criteria);
        if ( "$_monitor_update_query[3]" ne "0" ) {
                @_upd_key_columns       = split/,/, $_monitor_update_query[3];
                $_key_column = 1;
        } else { $_key_column = 0; }
        
        if ($_monitor_update_query[4] !~ "NULL" ) {
                $_exclude_values        = "YES";
                chomp($_monitor_update_query[4]);
                @_exclude_key_values    = split /<S>/, $_monitor_update_query[4];
        } else {$_exclude_values        = "NO";}

        $_exclude_upd_enable    = $_monitor_update_query[5]; chomp($_exclude_upd_enable);
        $_exclude_upd_set       = $_monitor_update_query[6]; chomp($_exclude_upd_set);
        $_upd_criteria          = $_monitor_update_query[7]; chomp($_upd_criteria);
        $_include_criteria      = $_monitor_update_query[8]; chomp($_include_criteria);
        $_exclude_criteria      = $_monitor_update_query[9]; chomp($_exclude_criteria);
        $_time = `date +"%m/%d/%Y %T"`; chomp($_time);
        print EVENT_TARCK "[ $_time ] : PREPARED THE UPDATE QUERY DETAILS\n";
}
print MAILOUT "${htmlstart}\n\t${trstart}\n\t\t";

$_time = `date +"%m/%d/%Y %T"`; chomp($_time);
print EVENT_TARCK "[ $_time ] : MAIL HEADER COLUMN PREPARATION IS STARTED\n";

foreach (@_m_columns) {
        chomp($_);
        print MAILOUT "${column_start}"."$_"."${column_end}\n\t\t";
}
if ( $_upd_enable =~ "Y" && $_key_column > 0 ) {
        print MAILOUT "${column_start}"."ACTION"."${column_end}\n\t\t";
}
print MAILOUT "${trend}\n\t";

$_time = `date +"%m/%d/%Y %T"`; chomp($_time);
print EVENT_TARCK "[ $_time ] : MAIL HEADER COLUMN PREPARATION IS COMPLETED\n";

foreach (@_query_output) {
        $_skip_status = "NO";
        $_tmp_update_value = "NO";
        next if ( $_ =~ "-----" );
        next if ( $_ =~ "rows selected" );
        next if ( $_ !~ "[0-9]" );      
        @_query_result = split /<SEP>/, $_;
        $_cnt = 1;
        $_hnoc_notify_temp = "N";
        $_noc_notify_temp = "N";
        $_ck = "";
        $_nk = "";
        $_exclude_match = "NO";

        if ( $_eml_below_thold =~ "N" ) {
                for (@_count_key_column) {
                        $_cel_cnt = $_; chomp($_cel_cnt); $_cel_cnt--;
                        if ( $_query_result[$_cel_cnt] < $_threshold ) {
                                $_skip_status = "YES";
                        } else {
                                $_skip_status = "NO";
                                last;
                        }
                }
        }

        next if ( $_eml_below_thold =~ "N" && $_skip_status =~ "YES" );
        
        $_skip_status = "NO";
        if ( $_if_count_zero =~ "N" ) {
                for (@_count_key_column) {
                        $_cel_cnt = $_; chomp($_cel_cnt); $_cel_cnt--;
                        if ( $_query_result[$_cel_cnt] == 0 ) {
                                $_skip_status = "YES";
                        } else {
                                $_skip_status = "NO";
                                last;
                        }
                }
        }

        next if ( $_if_count_zero =~ "N" && $_skip_status =~ "YES" );

        if ( $_upd_enable =~ "Y" && $_key_column > 0 ) {
                $_time = `date +"%m/%d/%Y %T"`; chomp($_time);
                print EVENT_TARCK "[ $_time ] : PREPARING THE UPDATE QUERY FOR THIS ROW --> '@_query_result'\n";
                $_key_criteria = "";
                foreach (@_upd_key_columns) {
                        $_key_criteria .= "AND $_m_columns[$_-1] = '$_query_result[$_-1]' "
                }
                
                if ( $_exclude_values =~ "YES" ) {
                        foreach (@_upd_key_columns) {
                                $_upd_key_tmp = $_;
                                foreach (@_exclude_key_values) {
                                        @_exclude_tmp = split /,/, $_;
                                        if ( "$_query_result[$_upd_key_tmp-1]" eq "$_exclude_tmp[$_upd_key_tmp-1]" ) {
                                                $_exclude_match = "YES";
                                                last;
                                        } 
                                        else { $_exclude_match = "NO"; }
                                }
                                last if ( $_exclude_match =~ "NO" );
                        }
                }
                
                if ( $_exclude_upd_enable =~ "Y" && $_exclude_match =~ "YES" ) {
                        `> $_reflow_query_file_tmp`;
                        if ( $_exclude_criteria !~ "NULL" ) {
                                print REFLOW_MONITOR_TMP "UPDATE $_table_name SET $_exclude_upd_set WHERE $_where_criteria $_key_criteria $_exclude_criteria;\n";
                                print REFLOW_MONITOR "UPDATE $_table_name SET $_exclude_upd_set WHERE $_where_criteria $_key_criteria $_exclude_criteria;\n";
                        } 
                        else {
                                print REFLOW_MONITOR_TMP "UPDATE $_table_name SET $_exclude_upd_set WHERE $_where_criteria $_key_criteria;\n";
                                print REFLOW_MONITOR "UPDATE $_table_name SET $_exclude_upd_set WHERE $_where_criteria $_key_criteria;\n";
                        }
                        print REFLOW_MONITOR_TMP "COMMIT;\n";
                        print REFLOW_MONITOR_TMP "exit;\n";
                        print REFLOW_MONITOR "COMMIT;\n";
                        print REFLOW_MONITOR "exit;\n";
                        $_tmp_update_value = "YES";
                }

                elsif ( $_exclude_match =~ "NO" ) {
                        `> $_reflow_query_file_tmp`;
                        if ( $_include_criteria !~ "NULL" ) {
                                print REFLOW_MONITOR_TMP "UPDATE $_table_name SET $_default_upd_set WHERE $_where_criteria $_key_criteria $_include_criteria;\n";
                                print REFLOW_MONITOR "UPDATE $_table_name SET $_default_upd_set WHERE $_where_criteria $_key_criteria $_include_criteria;\n";
                        }
                        else {
                                print REFLOW_MONITOR_TMP "UPDATE $_table_name SET $_default_upd_set WHERE $_where_criteria $_key_criteria;\n";
                                print REFLOW_MONITOR "UPDATE $_table_name SET $_default_upd_set WHERE $_where_criteria $_key_criteria;\n";
                        }
                        print REFLOW_MONITOR_TMP "COMMIT;\n";
                        print REFLOW_MONITOR_TMP "exit;\n";
                        print REFLOW_MONITOR "COMMIT;\n";
                        print REFLOW_MONITOR "exit;\n";
                        $_tmp_update_value = "YES";
                }

                elsif ( $_exclude_upd_enable =~ "N" && $_exclude_match =~ "YES" ) {
                        $_tmp_update_value = "NO";
                }
                $_time = `date +"%m/%d/%Y %T"`; chomp($_time);
                print EVENT_TARCK "[ $_time ] : PREPARED THE UPDATE QUERY FOR THIS ROW --> '@_query_result'\n";
        }

        print MAILOUT "${trstart}\n\t\t";
        foreach (@_query_result) {
                $_result_temp = $_ ; chomp($_result_temp);
                foreach (@_noc_key_column) {
                        if ( $_ eq $_cnt ) {
                                $_nk = ":$_result_temp:"."$_nk";
                        }
                }

                foreach (@_count_key_column) {
                        for (@_count_key_column) {
                                if ( $_ eq $_cnt ) {
                                        $_exist = "YES";
                                        last;
                                }
                                else {$_exist = "NO";}
                        }
                        if ( $_exist =~ "YES" && $_ eq $_cnt ) {
                                if ( $_result_temp < $_threshold ) {
                                        print MAILOUT "${mid_cel_start}"."$_result_temp"."${cel_end}\n\t\t";
                                        if ( $_tmp_update_value =~ "YES" && ( $_upd_criteria =~ "BT" || $_upd_criteria =~ "BHT" || $_upd_criteria =~ "ALL" ) ) {
                                                $_time = `date +"%m/%d/%Y %T"`; chomp($_time);
                                                print EVENT_TARCK "[ $_time ] : EXECUTING THE UPDATE QUERY FOR THIS ROW --> '@_query_result'\n";
                                                if ( $_upd_executed == 0 ) {
                                                        $_upd_executed = 1;
                                                        `> $_track_history_update`;
                                                        print UPDATE_TRACK "UPDATE MONITOR_HISTORY SET UPD_EXE_DT = SYSDATE, STATUS = 'UPD_EXECUTING' WHERE SNO = $_sno_seq_for_history;\n";
                                                        print UPDATE_TRACK "COMMIT;\n";
                                                        print UPDATE_TRACK "EXIT;\n";
                                                        `sqlplus -s $_repo_db[1]/$_repo_db[2]\@$_repo_db[3] \@$_track_history_update > /dev/null`;
                                                }
                                                $_reflow_query_output_tmp = `sqlplus -s $_db[1]/$_db[2]\@$_db[3] \@$_reflow_query_file_tmp | egrep 'rows updated|row updated'`; chomp($_reflow_query_output_tmp);
                                                $_reflow_query_output_tmp =~ s/ ..*//g;
                                                $_time = `date +"%m/%d/%Y %T"`; chomp($_time);
                                                print EVENT_TARCK "[ $_time ] : EXECUTED THE UPDATE QUERY FOR THIS ROW --> '@_query_result'\n";
                                        }
                                        elsif ( $_tmp_update_value =~ "YES" && $_upd_criteria !~ "BT" && $_upd_criteria !~ "BHT" && $_upd_criteria !~ "ALL" ) {
                                                $_reflow_query_output_tmp = "CHECK";
                                        }
                                }
                                elsif ( $_result_temp < $_h_threshold && $_result_temp >= $_threshold ) {
                                        print MAILOUT "${thr_cel_start}"."$_result_temp"."${thr_cel_end}\n\t\t";
                                        if ( $_noc_notify =~ "Y" ) {
                                                $_time = `date +"%m/%d/%Y %T"`; chomp($_time);
                                                print EVENT_TARCK "[ $_time ] : NOC NOTIFICATION IS TRUE FOR THIS ROW --> '@_query_result'\n";
                                                $_ck = ":$_result_temp:"."$_ck";
                                                $_noc_notify_temp = "Y";
                                        }
                                        if ( $_tmp_update_value =~ "YES" && ( $_upd_criteria =~ "BHT" || $_upd_criteria =~ "ALL" ) ) {
                                                $_time = `date +"%m/%d/%Y %T"`; chomp($_time);
                                                print EVENT_TARCK "[ $_time ] : EXECUTING THE UPDATE QUERY FOR THIS ROW --> '@_query_result'\n";
                                                if ( $_upd_executed == 0 ) {
                                                        $_upd_executed = 1;
                                                        `> $_track_history_update`;
                                                        print UPDATE_TRACK "UPDATE MONITOR_HISTORY SET UPD_EXE_DT = SYSDATE, STATUS = 'UPD_EXECUTING' WHERE SNO = $_sno_seq_for_history;\n";
                                                        print UPDATE_TRACK "COMMIT;\n";
                                                        print UPDATE_TRACK "EXIT;\n";
                                                        `sqlplus -s $_repo_db[1]/$_repo_db[2]\@$_repo_db[3] \@$_track_history_update > /dev/null`;
                                                }
                                                $_reflow_query_output_tmp = `sqlplus -s $_db[1]/$_db[2]\@$_db[3] \@$_reflow_query_file_tmp | grep 'rows updated'`; chomp($_reflow_query_output_tmp);
                                                $_reflow_query_output_tmp =~ s/ ..*//g;
                                                $_time = `date +"%m/%d/%Y %T"`; chomp($_time);
                                                print EVENT_TARCK "[ $_time ] : EXECUTED THE UPDATE QUERY FOR THIS ROW --> '@_query_result'\n";
                                        } 
                                        elsif ( $_tmp_update_value =~ "YES" && $_upd_criteria !~ "BHT" && $_upd_criteria !~ "ALL" ) {
                                                $_reflow_query_output_tmp = "CHECK";
                                        }
                                }
                                elsif ( $_result_temp >= $_h_threshold ) {
                                        print MAILOUT "${h_thr_cel_start}"."$_result_temp"."${h_thr_cel_end}\n\t\t";
                                        if ( $_h_noc_notify =~ "Y" ) {
                                                $_time = `date +"%m/%d/%Y %T"`; chomp($_time);
                                                print EVENT_TARCK "[ $_time ] : NOC NOTIFICATION [HIGH THRESHOLD] IS TRUE FOR THIS ROW --> '@_query_result'\n";
                                                $_ck = ":$_result_temp:"."$_ck";
                                                $_hnoc_notify_temp = "Y";
                                        }
                                        if ( $_tmp_update_value =~ "YES" && $_upd_criteria =~ "ALL" ) {
                                                $_time = `date +"%m/%d/%Y %T"`; chomp($_time);
                                                print EVENT_TARCK "[ $_time ] : EXECUTING THE UPDATE QUERY FOR THIS ROW --> '@_query_result'\n";
                                                if ( $_upd_executed == 0 ) {
                                                        $_upd_executed = 1;
                                                        `> $_track_history_update`;
                                                        print UPDATE_TRACK "UPDATE MONITOR_HISTORY SET UPD_EXE_DT = SYSDATE, STATUS = 'UPD_EXECUTING' WHERE SNO = $_sno_seq_for_history;\n";
                                                        print UPDATE_TRACK "COMMIT;\n";
                                                        print UPDATE_TRACK "EXIT;\n";
                                                        `sqlplus -s $_repo_db[1]/$_repo_db[2]\@$_repo_db[3] \@$_track_history_update > /dev/null`;
                                                }                                                
                                                $_reflow_query_output_tmp = `sqlplus -s $_db[1]/$_db[2]\@$_db[3] \@$_reflow_query_file_tmp | grep 'rows updated'`; chomp($_reflow_query_output_tmp);
                                                $_reflow_query_output_tmp =~ s/ ..*//g;
                                                $_time = `date +"%m/%d/%Y %T"`; chomp($_time);
                                                print EVENT_TARCK "[ $_time ] : EXECUTED THE UPDATE QUERY FOR THIS ROW --> '@_query_result'\n";
                                        }
                                        elsif ( $_tmp_update_value =~ "YES" && $_upd_criteria !~ "ALL" ) {
                                                $_reflow_query_output_tmp = "CHECK";
                                        }
                                }
                                $status = 1;
                                last;
                        }
                        elsif ($_exist =~ "NO") {
                                print MAILOUT "${cel_start}"."$_result_temp"."${cel_end}\n\t\t";
                                last;
                        }

                }
                $_cnt++;
        }
        if ( $_tmp_update_value =~ "YES" && $_upd_enable =~ "Y" ) {
                if ( $_exclude_match =~ "YES" && $_reflow_query_output_tmp !~ "CHECK" ) {
                        if ( $_reflow_query_output_tmp eq "0" || $_reflow_query_output_tmp eq "" ) {
                                print MAILOUT "${cel_start}"."0"."(ASIDE)"."${cel_end}\n\t\t";
                        }
                        else {print MAILOUT "${cel_start}"."$_reflow_query_output_tmp"."(ASIDE)"."${cel_end}\n\t\t";}
                } 
                elsif ( $_exclude_match =~ "NO" && $_reflow_query_output_tmp !~ "CHECK") {
                        if ( $_reflow_query_output_tmp eq "0" || $_reflow_query_output_tmp eq "" ) {
                                print MAILOUT "${cel_start}"."0"."(REFLOW)"."${cel_end}\n\t\t";
                        }
                        else {print MAILOUT "${cel_start}"."$_reflow_query_output_tmp"."(REFLOW)"."${cel_end}\n\t\t";}
                }
                else {
                        print MAILOUT "${cel_start}"."$_reflow_query_output_tmp"."${cel_end}\n\t\t";
                }
        }
        elsif ( $_tmp_update_value =~ "NO" && $_upd_enable =~ "Y" ) {
                print MAILOUT "${cel_start}"."N/A"."${cel_end}\n\t\t";
        }

        print MAILOUT "${trend}\n\t";
		$_noc_severity = 2;
        if ( $_h_noc_notify =~ "Y" || $_noc_notify =~ "Y" ) {
		$_time = `date +"%m/%d/%Y %T"`; chomp($_time);
                if ( $_hnoc_notify_temp =~ "Y" ) {
			$_noc_severity = 4;
                        print NOC_NOTIFY "[ $_time ] : <$_hostname><Application><OMS-$_noc_alert_number><$_h_noc_severity><$_m_subject><$_nk><$_ck Please check>\n";
                }
                elsif ( $_hnoc_notify_temp !~ "Y" && $_noc_notify_temp =~ "Y" ) {
			$_noc_severity = 3;
                        print NOC_NOTIFY "[ $_time ] : <$_hostname><Application><OMS-$_noc_alert_number><$_noc_severity><$_m_subject><$_nk><$_ck Please check>\n";
                }
        }
	my ($osm_db_name,$osm_queue_name) = split('\.',$_query_result[0]);	
	my $summary_datails = "Please check the QUEUE NAME : $osm_queue_name on the DATABASE : $osm_db_name Please see details below:\nWAITING : $_query_result[1]-\nREADY : $_query_result[2]-\nEXPIRED : $_query_result[3]-\nAVERAGE_WAIT : $_query_result[4]";
	my $logger_line = "/usr/bin/logger -p local5.err \"<$_hostname><Application><OMS-$_noc_alert_number><$_noc_severity><HOMSAlert-$_m_subject><$_query_result[0]><$summary_datails>\"";
	system("$logger_line");
	#print "Logger Command --> $logger_line\n";
}

if ( $_upd_executed == 1 ) {
        `> $_track_history_update`;
        print UPDATE_TRACK "UPDATE MONITOR_HISTORY SET UPD_EXE_COMP_DT = SYSDATE, STATUS = 'UPD_EXECUTED' WHERE SNO = $_sno_seq_for_history;\n";
        print UPDATE_TRACK "COMMIT;\n";
        print UPDATE_TRACK "EXIT;\n";
        `sqlplus -s $_repo_db[1]/$_repo_db[2]\@$_repo_db[3] \@$_track_history_update > /dev/null`;
}

print MAILOUT "${htmlend}";

print MAILOUT qq#<font style='font-family:"Calibri";font-size:15px;'><u><b>Monitor Query:</b></u><i> [$_db[1]\@$_db[3]]<i></font><br>#;
print MAILOUT qq#<i><font style='font-family:"Calibri";font-size:12px;'>$_m_help_query</font><i>\n\n#;

if ( "$_confluence" =~ "http" ) {
        print MAILOUT qq#<font style='font-family:"Calibri";font-size:15px;'><u><b>Confluence Page:</b></u><i> [Please use your LDAP credential to view below link]<i></font><br>#;
        print MAILOUT qq#<font style='font-family:"Calibri";font-size:14px;'>$_confluence</font>\n#;
}

$_time = `date +"%m/%d/%Y %T"`; chomp($_time);
print EVENT_TARCK "[ $_time ] : MAIL PREPARATION IS DONE\n";

close MAILOUT;

if ( $status > 0 ) {
        `(echo "Subject: $_m_subject" ; echo "To: $_mail_to" ; echo "cc: $_mail_cc" ; echo "MIME-Version: 1.0" ; echo "Content-Type: text/html" ; echo "Content-Disposition: inline" ; echo '<HTML><BODY><PRE>' ; cat $_mail_out_file; echo '</PRE></BODY></HTML>' ; ) | /usr/sbin/sendmail -i -t`;
        `mv $_mail_out_file $_sent_dir` ;
        $_time = `date +"%m/%d/%Y %T"`; chomp($_time);
        #print EVENT_TARCK "[ $_time ] : EMAIL IS SENT TO: $_mail_to AND CC: $_mail_cc\n";
        `> $_track_history_update`;
        print UPDATE_TRACK "UPDATE MONITOR_HISTORY SET COMPLETE_DT = SYSDATE, STATUS = 'COMPLETED',  RIP_STATUS = 'Y', EMAIL_STATUSS = 'SENT' WHERE SNO = $_sno_seq_for_history;\n";
        print UPDATE_TRACK "COMMIT;\n";
        print UPDATE_TRACK "EXIT;\n";
        `sqlplus -s $_repo_db[1]/$_repo_db[2]\@$_repo_db[3] \@$_track_history_update > /dev/null`;
} else {
        $_time = `date +"%m/%d/%Y %T"`; chomp($_time);
        print EVENT_TARCK "[ $_time ] : NO EMAIL NEED TO SEND\n";
        `> $_track_history_update`;
        print UPDATE_TRACK "UPDATE MONITOR_HISTORY SET COMPLETE_DT = SYSDATE, STATUS = 'COMPLETED' WHERE SNO = $_sno_seq_for_history;\n";
        print UPDATE_TRACK "COMMIT;\n";
        print UPDATE_TRACK "EXIT;\n";
        `sqlplus -s $_repo_db[1]/$_repo_db[2]\@$_repo_db[3] \@$_track_history_update > /dev/null`;
}

print UPD_MONITOR_STATUS "UPDATE OSS_MONITORS SET STATUS = 'NO EMAIL' WHERE M_NUMBER = '$_m_number' AND DISABLED = 0;\n" if ( $status == 0 );
print UPD_MONITOR_STATUS "UPDATE OSS_MONITORS SET STATUS = 'EMAIL' WHERE M_NUMBER = '$_m_number' AND DISABLED = 0;\n" if ( $status > 0 );
print UPD_MONITOR_STATUS "UPDATE OSS_MONITORS SET LAST_RAN_DT = SYSDATE WHERE M_NUMBER = '$_m_number' AND DISABLED = 0;\n";
print UPD_MONITOR_STATUS "UPDATE OSS_UPD_MONITOR SET LAST_UPDATE_RAN = SYSDATE WHERE M_NUMBER = '$_m_number';\n" if ( $_tmp_update_value =~ "YES" && $_upd_enable =~ "Y" );
print UPD_MONITOR_STATUS "COMMIT;\n";
print UPD_MONITOR_STATUS "exit;\n";
close UPD_MONITOR_STATUS;

`sqlplus -s $_repo_db[1]/$_repo_db[2]\@$_repo_db[3] \@$_monitor_run_status_upd_file > /dev/null`;
`rm -frR $_track_history_update`;

$_time = `date +"%m/%d/%Y %T"`; chomp($_time);
print EVENT_TARCK "[ $_time ] : TOOL EXECUTION IS COMPLETED\n";
