#!/usr/bin/perl
use warnings;
use strict;
use DBI;
use Data::Dumper;
use Time::Seconds;
use Time::Piece;

my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
my $today_date =  sprintf("%02d-%02d-%04d", $mday, $mon+1, $year+1900);
########################################### MYSQL CONNECTION #######################################
my $driver = "mysql";           ########### Database Briver Name ################
my $database = "pursway";       ########### Data Base Name ######################
my $dsn = "DBI:$driver:database=$database"; ########### DSN Info For Connection ###############
my $userid = "root";                        ########### Data Base Login User Name #############
my $password = 'root';                          ########### Data Base Login PAssword ##############
my $dbh = DBI->connect($dsn, $userid, $password ) or die $DBI::errstr; ########## Connection Handler ############
####################################################################################################

my $report_hash = {
                        'sca'   =>      'SCA',
                        'sme'   =>      'SME',
                        'sel'   =>      'SEL',
                        'snei'  =>      'SNEI',
                        'spe'   =>      'SPE',
                        'sphe'  =>      'SPHE',
                   };
my ($reportfile,$cond_str,$date_str) = ('','',''); 
checkCommandLineArgument(@ARGV);

if(scalar @ARGV  >= 2){
	$date_str = "and date(created_date) = '$ARGV[1]'" if(scalar @ARGV  == 2);
	$date_str = "and date(created_date) >= '$ARGV[1]' and date(created_date) <= '$ARGV[2]'" if(scalar @ARGV  == 3);
}

if($report_hash->{$ARGV[0]} ne ''){
	$cond_str = "and project_name ='$report_hash->{$ARGV[0]}' $date_str";
}
else{
	print "Please Pass Correct Argument !!\n";
}

open(FILEVAR,">$reportfile")|| die "Unable to create report file, $!";
print FILEVAR  "PROCESS NAME\tPROJECT NAME\tFILE NAME\tIS FILE EXIST\tIS DOWNLOADED\tDOWNLOAD SOURCE\tIS UNCOMPRESSED\tIS ACT SCRIPT EXECUTED\tIS UTF CONVERTED\tRECORD COUNT\tIS PSQL OLD EXECUTED\tIS PSQL TRUNCATE\tIS PSQL DISTINCT EXECUTED\tIS META SCRIPT EXECUTED\tIS META PSQL OLD FAILD\tIS META PSQL TRUNCATE FAILED\tIS META PSQL DISTINCT FAILED\tPROCESS STATUS\n";

my $selectquery = qq{select * from SCA_pursway_logs where 1 $cond_str};
my $sth = $dbh->prepare($selectquery);
$sth->execute();
while(my @purswayData = $sth->fetchrow_array()){
	my($process_name,$project_name,$file_name,$is_file_exist,$is_downloaded,$d_source,$is_uncomp,$is_act,$is_utf,$recordcount,$is_psql_old,$is_psql_truncate,$is_psql_distinct,$is_meta_script,$is_meta_psql_old_failed,$is_meta_psql_trun_failed,$is_meta_psql_disticnt_failed,$p_status) = ('-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-');
	
	if($purswayData[1] eq 'ACT' || $purswayData[1] eq 'MEDIA'){
		($process_name,$project_name,$file_name,$is_file_exist,$is_downloaded,$d_source,$is_uncomp,$is_act,$is_utf,$recordcount,$is_psql_old,$is_psql_truncate,$is_psql_distinct) = ($purswayData[1],$purswayData[2],$purswayData[4],$purswayData[5],$purswayData[7],$purswayData[8],$purswayData[9],$purswayData[10],$purswayData[11],$purswayData[12],$purswayData[13],$purswayData[14],$purswayData[15]);
		if($is_psql_old == '1' &&  $is_psql_truncate =='1' && $is_psql_distinct =='1'){
                	$p_status = 'SUCCESS';
        	}
       	 	else{
                 	$p_status = 'FAILED';
        	}
		print FILEVAR "$process_name\t$project_name\t$file_name\t$is_file_exist\t$is_downloaded\t$d_source\t$is_uncomp\t$is_act\t$is_utf\t$recordcount\t$is_psql_old\t$is_psql_truncate\t$is_psql_distinct\t$is_meta_script\t$is_meta_psql_old_failed\t$is_meta_psql_trun_failed\t$is_meta_psql_disticnt_failed\t$p_status\n";
	}        

	if($purswayData[1] eq 'META'){
		 ($process_name,$project_name,$file_name,$is_file_exist,$is_downloaded,$d_source,$is_uncomp,$is_psql_old,$is_psql_truncate,$is_psql_distinct,$is_meta_script,$is_meta_psql_old_failed,$is_meta_psql_trun_failed,$is_meta_psql_disticnt_failed) = ($purswayData[1],$purswayData[2],$purswayData[4],$purswayData[5],$purswayData[7],$purswayData[8],$purswayData[9],$purswayData[13],$purswayData[14],$purswayData[15],$purswayData[17],$purswayData[18],$purswayData[19],$purswayData[20]);
		if($is_psql_old == 1 &&  $is_psql_truncate == 1 && $is_psql_distinct == 1 && $is_meta_psql_old_failed == 0 && $is_meta_psql_trun_failed ==0 && $is_meta_psql_disticnt_failed == 0 ){
                        $p_status = 'SUCCESS';
                }
                else{
                        $p_status = 'FAILED';
                }
		print FILEVAR  "$process_name\t$project_name\t$file_name\t$is_file_exist\t$is_downloaded\t$d_source\t$is_uncomp\t$is_act\t$is_utf\t$recordcount\t$is_psql_old\t$is_psql_truncate\t$is_psql_distinct\t$is_meta_script\t$is_meta_psql_old_failed\t$is_meta_psql_trun_failed\t$is_meta_psql_disticnt_failed\t$p_status\n";
        }
	
}

close FILEVAR;
sub checkCommandLineArgument {
        my(@ARGMENT)    =       @_;
        if(scalar @ARGMENT >= 1){
                if(!exists $report_hash->{$ARGMENT[0]}){
                        print "Project Name is Not defined Correctly, Please provide Correct Project Name in Argument!!\n";
                        exit;
                }
                $reportfile = "purswayreport/$report_hash->{$ARGMENT[0]}_report_$today_date.txt";
        }
        else{
                print "\nPlease Pass one Required Argument 1 -> Project Name !!\n";
                print "usage:\n1: Pass the Argument which Process & Project want to Run \neg. perl sriptname arg1* arg2 arg3 \nARG1* : Project Name(sca -> SCA,sme -> SME,sel -> SEL,snei -> SNEI,spe -> SPE,sphe -> SPHE)\nARG3  : Date -: if you want to run for a specific date then pass [yyyy-mm-dd] format\nARG4  : Date -: if you want to run between two date(arg3 & arg4) [yyyy-mm-dd] format\n\n";
                exit;
        }
}

