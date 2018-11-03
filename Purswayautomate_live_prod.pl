#!/usr/bin/perl
use warnings;
use strict;
use DBI;
use Time::Piece;
use Net::FTP;
use Data::Dumper;
use Time::Seconds;
use Net::SFTP::Foreign;
use autodie;
use Net::SMTP::SSL;
use MIME::Base64;


my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
my $now = sprintf("%02d_%02d_%04d_%02d_%02d_%02d", $mday, $mon+1, $year+1900, $hour, $min, $sec);
my $time = sprintf("%02d-%02d-%04d %02d:%02d:%02d", $mday, $mon+1, $year+1900, $hour, $min, $sec);
my $today_date =  sprintf("%02d-%02d-%04d", $mday, $mon+1, $year+1900);

my $project_hash = {
                        'sca'	=>	'SCA',
                        'sme'	=>	'SME',
                        'sel'	=>	'SEL',
                        'snei'	=>	'SNEI',
                        'spe'	=>	'SPE',
                        'sphe'	=>	'SPHE'
                   };
my ($logfile,$new_file_download_date);
checkCommandLineArgument(@ARGV);
my $file_list = "purswaylogs/$project_hash->{$ARGV[1]}_pursway_downloaded_files.txt";####### file for ftp file downloaded 

#######################################################################################################
my $act_path 		= purswayActLocalDirectory($project_hash->{$ARGV[1]});  ####### Act File Local Path #########
my $impresion_path 	= purswayMediaLocalDirectory($project_hash->{$ARGV[1]}); ###### Media File Local Path ########
my $meta_path 		= purswayMetaLocalDirectory($project_hash->{$ARGV[1]}); ####### Meta File Local Path ########

##################FTP Details################################################################
my $host  = '10.5.2.77';                  		######## FTP Host ################### 
my $user  = 'Username';                   		######## FTP User Name ##############
my $pass  = 'password';                   		######## FTP Passward ###############
my $dir   = ftpSourceDirPath($project_hash->{$ARGV[1]});#### FTP Source File Directory ######

######################Script Details##############
#This are external helper scripts to automated manual process
#q_string_splitter.pl - is used to split query string encoded urs to individual columns
my $actPerlScript   = 'q_string_splitter.pl'; ############# Perl Script For Act Process################
my $metaMergeScript	= metaMergeShellScript($project_hash->{$ARGV[1]}); ### Shell Script For Meta File Merge Process ####

#################################################
my $actSource	= actFileDownloadSource($project_hash->{$ARGV[1]});   #### Act File Download Source Location ######
my $mediaSource = mediaFileDownloadSource($project_hash->{$ARGV[1]}); #### Media File Download Source Location ####
my $metaSource  = metaFileDownloadSource($project_hash->{$ARGV[1]});  #### Meta File Download Source Location #####
#####################################################################################################

###############################SFTP Details#########################################################
my $sftp_dir 	= sftpSourceDirPath($project_hash->{$ARGV[1]});  ######## SFTP Dir Path #################
my $sftp_host = "199.38.164.171";	########### SFTP Host Name ################ 
my $sftp_user = "sca";          ########### SFTP Login User Name #################
my $sftp_pass = "k33p1tr\@3l";  ########### SFTP Password ########################
####################################################################################################

########################################### MYSQL CONNECTION #######################################
my $driver = "mysql";           ########### Database Briver Name ################ 
my $database = "pursway";       ########### Data Base Name ######################
my $dsn = "DBI:$driver:database=$database"; ########### DSN Info For Connection ###############
my $userid = "root";                        ########### Data Base Login User Name #############
my $password = 'root';                      ########### Data Base Login PAssword ##############
my $dbh = DBI->connect($dsn, $userid, $password ) or die $DBI::errstr; ########## Connection Handler ############
##################################################################################################

############################ Check Source Value Define for All Process to Download ######################### 
if($actSource eq ''){
	print "Please Provide Act Source Value To Download File !\n";
	exit;
}
if($mediaSource eq ''){
	print "Please Provide Media Source Value To Download File !\n";
	exit;
}
if($metaSource eq ''){
	print "Please Provide Meta Source Value To Download File !\n";
        exit;
}
############################################################################################################

######################### Check @nd Argument of Date and Convert it in Required Format #####################

my $userInputDate = '';
if(scalar @ARGV >= 3){
	chomp $ARGV[2];
	my $userDate = $ARGV[2];
	if($userDate ne ''){	
        	if($userDate =~/^\d{4}\-\d{2}\-\d{2}/){
                	print "Entered Date through commnad line for Execution ::$userDate\n";
                	$userInputDate = join('',split('-',$userDate));
        	}
        	else{
                	print "Please Enter Date in Correct Format [YYYY-MM-DD]\n";
                	exit;
        	}
	}
}

############################################################################################################

###################################### Create Log File #####################################################
chomp $ARGV[0] if($ARGV[0] ne '');
chomp $ARGV[1] if($ARGV[1] ne '');
open(LOGHANDLER,">$logfile")|| die "Unable to Create a Log file , $!";
open(FILELIST,">>$file_list")|| die "Unable to Create a  download file list , $!";
print "FILENAME|DOWNLOAD TIME\n"if(! -e $file_list);

############################################################################################################

purswayLogs("============================PURSWAY FILES PROCESSING INITIATED=======================!!\n");

##################################### Calling Process individually or All #################################
my($logInsertId,$file_check);
if($ARGV[0] eq 'act'){
	if(scalar @ARGV == 4 && $ARGV[3] eq '-F'){
		purswayLogs("Process -> ACT::Project -> $project_hash->{$ARGV[1]}::Date -> $userInputDate file processed forcefully to Download !\n");
		activityFileProcessExecution($userInputDate,$ARGV[0],$project_hash->{$ARGV[1]});
	}else{
		$file_check   = checkFileExecuteStatus($userInputDate,$ARGV[0],$project_hash->{$ARGV[1]});
		if($file_check =='0'){
			$logInsertId  = purswayLogInsert($dbh,'ACT',$project_hash->{$ARGV[1]});
			activityFileProcessExecution($userInputDate,$ARGV[0],$project_hash->{$ARGV[1]});
		}else{
		 	purswayLogs("Process -> ACT::Project -> $project_hash->{$ARGV[1]}::Date -> $userInputDate Already Downloaded !\n");
		}
	} 
}
elsif($ARGV[0] eq 'media'){
	if(scalar @ARGV == 4 && $ARGV[3] eq '-F'){
		purswayLogs("Process -> MEDIA::Project -> $project_hash->{$ARGV[1]}::Date -> $userInputDate file processed forcefully to Download !\n");
		mediaFileProcessExecution($userInputDate,$ARGV[0],$project_hash->{$ARGV[1]});
	}else{
		$file_check   = checkFileExecuteStatus($userInputDate,$ARGV[0],$project_hash->{$ARGV[1]});
		if($file_check =='0'){
			$logInsertId  = purswayLogInsert($dbh,'MEDIA',$project_hash->{$ARGV[1]});
			mediaFileProcessExecution($userInputDate,$ARGV[0],$project_hash->{$ARGV[1]}); 
		}else{
			purswayLogs("Process -> MEDIA::Project -> $project_hash->{$ARGV[1]}::Date -> $userInputDate Already Downloaded !\n");
		}
	}
}
elsif($ARGV[0] eq 'meta'){
	if(scalar @ARGV == 4 && $ARGV[3] eq '-F'){
		purswayLogs("Process -> META::Project -> $project_hash->{$ARGV[1]}::Date -> $userInputDate file processed forcefully to Download !\n");
		metaFileProcessExecution($userInputDate,$ARGV[0],$project_hash->{$ARGV[1]});
	}else{
		$file_check   = checkFileExecuteStatus($userInputDate,$ARGV[0],$project_hash->{$ARGV[1]});
		if($file_check =='0'){
			$logInsertId  = purswayLogInsert($dbh,'META',$project_hash->{$ARGV[1]});
			metaFileProcessExecution($userInputDate,$ARGV[0],$project_hash->{$ARGV[1]});
		}else{
			purswayLogs("Process -> META::Project -> $project_hash->{$ARGV[1]}::Date -> $userInputDate Already Downloaded !\n");
		}
	}
}
elsif($ARGV[0] eq 'all'){					#### Calling Function For All  Procees Execution #########
	if(scalar @ARGV == 4 && $ARGV[3] eq '-F'){
		purswayLogs("Process -> ACT::Project -> $project_hash->{$ARGV[1]}::Date -> $userInputDate file processed forcefully to Download !\n");
                activityFileProcessExecution($userInputDate,$ARGV[0],$project_hash->{$ARGV[1]});
		sleep(1);
		purswayLogs("Process -> MEDIA::Project -> $project_hash->{$ARGV[1]}::Date -> $userInputDate file processed forcefully to Download !\n");
                mediaFileProcessExecution($userInputDate,$ARGV[0],$project_hash->{$ARGV[1]});
		sleep(1);
		purswayLogs("Process -> META::Project -> $project_hash->{$ARGV[1]}::Date -> $userInputDate file processed forcefully to Download !\n");
                metaFileProcessExecution($userInputDate,$ARGV[0],$project_hash->{$ARGV[1]});
		
	}else{
		$file_check   = checkFileExecuteStatus($userInputDate,'act',$project_hash->{$ARGV[1]});
		if($file_check =='0'){
			$logInsertId  = purswayLogInsert($dbh,'ACT',$project_hash->{$ARGV[1]});
			activityFileProcessExecution($userInputDate,$ARGV[0],$project_hash->{$ARGV[1]});
		}else{
			purswayLogs("Process -> ACT::Project -> $project_hash->{$ARGV[1]}::Date -> $userInputDate Already Downloaded !\n");
		}
		sleep(1);
		$file_check   = checkFileExecuteStatus($userInputDate,'media',$project_hash->{$ARGV[1]});
		if($file_check =='0'){
			$logInsertId  = purswayLogInsert($dbh,'MEDIA',$project_hash->{$ARGV[1]});
			mediaFileProcessExecution($userInputDate,$ARGV[0],$project_hash->{$ARGV[1]});
		}else{
			purswayLogs("Process -> MEDIA::Project -> $project_hash->{$ARGV[1]}::Date -> $userInputDate Already Downloaded !\n");
		}
		sleep(1);
		$file_check   = checkFileExecuteStatus($userInputDate,'meta',$project_hash->{$ARGV[1]});
		if($file_check =='0'){
			$logInsertId  = purswayLogInsert($dbh,'META',$project_hash->{$ARGV[1]});
			metaFileProcessExecution($userInputDate,$ARGV[0],$project_hash->{$ARGV[1]});
		}else{
			purswayLogs("Process -> META::Project -> $project_hash->{$ARGV[1]}::Date -> $userInputDate Already Downloaded !\n");
		}
	}
}
else{
	print "Please Provide Input From Command Line to Execute a Process. !\n";
	exit;
}

####################### Function For Activity Process #############################################
#Note here ftp_filename variable name signifies source which can be ftp,sftp,gs,s3 etc

sub activityFileProcessExecution{
	my($cmdDate,$pflag,$actprojectFlag) 	= @_;
	# Here the LastFileName is used to determine which file to download; however its not used when date parameter is passed
        # via command line argument
	my $last_file  = lastFileName($act_path,'act_','Activity',$pflag);
	chomp $last_file;
	if($last_file ne ''||$cmdDate ne ''){
		purswayLogUpdate($dbh,$logInsertId,'last_filename',$last_file)if($last_file ne '');	
		#my $fdate = FindNextDate($last_file);
		my $fdate;
		if($cmdDate ne ''){
			$fdate = $cmdDate;
		}
		else{
			$fdate = FindNextDate($last_file);
		}
		$new_file_download_date = $fdate;
		my $ftp_file_name = "act_$fdate*";
		#y $ftp_file_name = "act_20151211*";
		my $new_file_name = originalFileName($ftp_file_name);
		purswayLogUpdate($dbh,$logInsertId,'new_filename',$new_file_name);
		my @ftp_lists = checkFileListExist($ftp_file_name,$actSource,$act_path);
		if(@ftp_lists){
			purswayLogUpdate($dbh,$logInsertId,'is_file_exist','1');
			my $status = FileListDownload($ftp_file_name,$actSource,$act_path,'act');
			my $uncomp = fileUncompress($act_path,'act');
			if($uncomp ==0){
				purswayLogUpdate($dbh,$logInsertId,'is_file_uncompessed','1');
				purswayLogs("Files Uncompressing completed successfully !\n");
				purswayLogs("Issuing Perl Script to verify q_string column in activity data !\n");
				my $perl_out =  `perl $actPerlScript $actprojectFlag`;
				if($? ==0 && $perl_out !~ m/ERROR/g ||$perl_out !~ m/Error/g){
					purswayLogUpdate($dbh,$logInsertId,'is_act_script_executed','1');
					actMediaMetaCommonProcess('act',$act_path,'act_new.txt',$pflag,$actprojectFlag);
   			    	}
                        	else{
					purswayMailError("ERROR :: Project :: $actprojectFlag File Type :: $ftp_file_name"," Perl Script for Act Process Failed , ERRRO :: $perl_out!!");
					purswayLogs("Perl Script for Act Process Failed !:: $perl_out\n");
                        	}
			}
			else{
				purswayMailError("ERROR :: Project :: $actprojectFlag File Type :: $ftp_file_name","Act Files Uncompressing Failed for $ftp_file_name!!");
				purswayLogs("Act Files Uncompressing Failed !\n");
			}	
		}
		else{
			purswayMailError("ERROR :: Project :: $actprojectFlag File Type :: $ftp_file_name"," No New File Found to Download for ACT from SFTP Server!!");
			purswayLogs("No New File Found to Download for ACT from SFTP Server !\n");
		}        
	}
	else{
		purswayMailError("ERROR :: Project :: $actprojectFlag File Type ::Activity"," No File Found in Activity Local Directory  !!");
		purswayLogs("No File Found in Activity Local Directory !\n");
	}
	purswayLogs("=============================================================================\n\n");
}
###########################################################################################################

################################ Function For Media Process ###############################################

sub mediaFileProcessExecution {
	 my($media_cmdDate,$media_pflag,$mediaProjectFlag)     = @_;

	purswayLogs("=============================================================================\n\n");	
        my $m_last_file  = lastFileName($impresion_path,'media_','Media',$media_pflag);
	if($m_last_file ne ''||$media_cmdDate ne ''){
		#my $m_fdate = FindNextDate($m_last_file);
		purswayLogUpdate($dbh,$logInsertId,'last_filename',$m_last_file)if($m_last_file ne '');
		my $m_fdate;
                if($media_cmdDate ne ''){
                        $m_fdate = $media_cmdDate;
                }
                else{
                        $m_fdate = FindNextDate($m_last_file);
                }
		$new_file_download_date = $m_fdate;
		my $m_ftp_file_name = "media_$m_fdate*";
                #my $m_ftp_file_name = "media_20160206*";
		my $m_new_file_name = originalFileName($m_ftp_file_name);
                purswayLogUpdate($dbh,$logInsertId,'new_filename',$m_new_file_name);
                my @m_ftp_lists = checkFileListExist($m_ftp_file_name,$mediaSource,$impresion_path);
		if(@m_ftp_lists){	
			purswayLogUpdate($dbh,$logInsertId,'is_file_exist','1');	
                        my $m_status =FileListDownload($m_ftp_file_name,$mediaSource,$impresion_path,'media');		
			      my $uncomp_media = fileUncompress($impresion_path,'media');
			if($uncomp_media ==0){
				purswayLogUpdate($dbh,$logInsertId,'is_file_uncompessed','1');
				purswayLogs("Files Uncompressing completed successfully !\n");
				purswayLogs("Merging All the Impression files Together !\n");
				`cat $impresion_path/media_* >> $impresion_path/mediafile_updated`;			
				purswayLogUpdate($dbh,$logInsertId,'is_act_script_executed','1');			
				my $m_result = actMediaMetaCommonProcess('media',$impresion_path,'mediafile_updated',$media_pflag,$mediaProjectFlag);
                        }
                        else{
				purswayMailError("ERROR :: Project :: $mediaProjectFlag File Type :: $m_ftp_file_name","Media Files Uncompressing Failed for $m_ftp_file_name !!");
				purswayLogs("Media Files Uncompressing Failed !\n");
                        }		
		}
		else{
			 purswayMailError("ERROR :: Project :: $mediaProjectFlag File Type :: $m_ftp_file_name"," No New File Found to Download for MEDIA from SFTP Server!!");
			 purswayLogs("No New File Found to Download for Media from SFTP Server !\n");
		}
	}
	else{
		purswayMailError("ERROR :: Project :: $mediaProjectFlag File Type :: MEDIA ","No File Found in Media Local Directory!!");	
		purswayLogs("No File Found in Media Directory !\n");
	}
	purswayLogs("====================================================================================\n\n");
}
############################################################################################################

################################# Function For Meta process ################################################	
sub metaFileProcessExecution {
	my($meta_cmdDate,$meta_pflag,$metaProjectFlag)     = @_;

        purswayLogs("====================================================================================\n\n");
	my $meta_last_file  = lastFileName($meta_path,'meta_','Meta',$meta_pflag);
	if($meta_last_file ne ''|| $meta_cmdDate ne ''){
		purswayLogUpdate($dbh,$logInsertId,'last_filename',$meta_last_file)if($meta_last_file ne '');
                #my $meta_fdate = FindNextDate($meta_last_file);
		my $meta_fdate;
                if($meta_cmdDate ne ''){
                        $meta_fdate = $meta_cmdDate;
                }
                else{
                        $meta_fdate = FindNextDate($meta_last_file);
                }
		my $meta_ftp_file_name = "meta_$meta_fdate*";
                #my $meta_ftp_file_name = "meta_20160104*";
		my $meta_new_file_name = originalFileName($meta_ftp_file_name);
                purswayLogUpdate($dbh,$logInsertId,'new_filename',$meta_new_file_name);
                my @meta_ftp_lists = checkFileListExist($meta_ftp_file_name,$metaSource,$meta_path);
		if(@meta_ftp_lists){
			purswayLogUpdate($dbh,$logInsertId,'is_file_exist','1');
                        my $meta_status =FileListDownload($meta_ftp_file_name,$metaSource,$meta_path,'meta');            
			my $uncomp_meta = fileUncompress($meta_path,'meta');
                        if($uncomp_meta ==0){
				purswayLogUpdate($dbh,$logInsertId,'is_file_uncompessed','1');
				purswayLogs("Files Uncompressing completed successfully !\n");
				purswayLogs("Merging meta files together !\n");
				my $meta_output = system("sh $metaMergeScript");
				if($meta_output =='0'){
					purswayLogUpdate($dbh,$logInsertId,'is_meta_script_executed','1');
					purswayLogs("Push Meta Data to Postgres !\n");
					my $meta_file_psql = metaPSQLCommand($metaProjectFlag);
                                        open(FILEVAR,"$meta_file_psql") || die "unable to open $!";
					my $meta_out1 = metaPSQLExecution('FILEVAR');
					if($meta_out1 ==1){
						metaPSQLTruncate($metaProjectFlag);
						metaPSQLInsert($metaProjectFlag);	
						actMediaMetaCommonProcess('meta',$meta_path,'',$meta_pflag,'');
					}
					else{
						purswayMailError("ERROR :: Project :: $metaProjectFlag File Type ::$meta_ftp_file_name ","Meta Data Push Failed through PSQL in Old Table for $meta_ftp_file_name!!");
						purswayLogs("Meta Data Push Failed through PSQL in Old Table!\n");
					}
				}else{
					purswayMailError("ERROR :: Project :: $metaProjectFlag File Type :: $meta_ftp_file_name ","Meta Data File Merging Failed for $meta_ftp_file_name!!");
					purswayLogs("Meta Data File Merging Failed !\n");
				}				
                        }
                        else{
				purswayMailError("ERROR :: Project :: $metaProjectFlag File Type :: $meta_ftp_file_name ","Meta Files Uncompressing Failed for $meta_ftp_file_name !!");
				purswayLogs("Meta Files Uncompressing Failed !\n");
                        }
                }
		else{
			purswayMailError("ERROR :: Project :: $metaProjectFlag File Type :: $meta_ftp_file_name"," No New File Found to Download for META from SFTP Server!!");
			purswayLogs("No New File Found to Download for Meta from SFTP Server !\n");
                }
	}
	else{
		purswayMailError("ERROR :: Project :: $metaProjectFlag File Type :: META ","No File Found in Metafiles Local Directory!!");
		purswayLogs("No File Found in Metafiles Directory !\n");
        }
	purswayLogs("====================================================================================\n\n");
}
###########################################################################################################

################################# Common Steps For All Process ############################################

sub actMediaMetaCommonProcess{
	my($p_type,$a_path,$output_filename,$all_stop_flag,$all_project_flag) = @_;
        if($p_type eq 'meta'){
		 purswayLogs("Meta Data has been pushed successfully !\n");
		 purswayLogs("Deleting Logs and Extracted Data !\n");
                 `rm $a_path/*.log $a_path/*.txt`;
		 purswayLogUpdate($dbh,$logInsertId,'is_meta_delete_logs','1')if($?==0);
		 purswayLogs("Meta file processing completed successfully !\n");
		 purswayLogs("End Time For Meta File Process==========================================$time\n");
		 purswayLogs("Stop Time For All process================================$time\n\n")if($all_stop_flag eq 'all');
	}
	else{	
		if($p_type eq 'act'){
			purswayLogs("Perl Script Executed Successfully for Activity Data !\n");
		}
		my $fconversion = actMediaUTFConversion($a_path,$output_filename,$p_type);
		if($fconversion ==0){
			$output_filename = 'new_act_updated' if($p_type eq 'act');
			$output_filename = 'mediafile_updated_new' if($p_type eq 'media');
			purswayLogUpdate($dbh,$logInsertId,'is_utf_converted','1');
			purswayLogs("UTF-8 File conversion successful with new file name :: $output_filename !\n");
			my $recordCount = `cat $a_path/$output_filename | wc -l`;
			purswayLogUpdate($dbh,$logInsertId,'file_record_count',$recordCount);
			purswayLogs("Number of Line in File ($output_filename)::$recordCount !\n");
			purswayLogs("Push the data to Postgres !\n");
			actMediaPsqlTruncateExecution($p_type,$all_project_flag);
			my $psql_actmedia = actMediaPsqlExecution($p_type,$all_project_flag);
			if($psql_actmedia==0){
				actMediaPsqlDistinctExecution($p_type,$all_project_flag);
				actMediaPsqlTruncateExecution($p_type,$all_project_flag);
				`mv $a_path/$output_filename $a_path/$output_filename.$new_file_download_date`;
				purswayLogUpdate($dbh,$logInsertId,'is_psql_executed','1');
				purswayLogs("Data has been pushed successfully Through PSQL! \n");
				my $compress_file = $p_type."_20*";
		        	my $fcompress	= system("gzip $a_path/$compress_file");
				`gzip $a_path/$output_filename.$new_file_download_date`;
				`mv $a_path/$output_filename.$new_file_download_date* $a_path/tmp/`;
				if($fcompress ==0){
					purswayLogUpdate($dbh,$logInsertId,'is_file_compressed','1');
					purswayLogs("Compressing $p_type files Successfully !\n");
					purswayLogs("$p_type file processing completed successfully !\n");
					purswayLogs("End Time For $p_type File Process================================================ $time\n");
				}
				else{
					purswayMailError("ERROR :: Project :: $all_project_flag File Type :: $p_type ","Compressing $p_type files Failed!!");
					purswayLogs("Compressing $p_type files Failed !\n");
				}
                	}
                	else{
				 purswayMailError("ERROR :: Project :: $all_project_flag File Type :: $p_type ","Data pushed Failed Trough PSQL in Old Table fro $p_type ! ERROR :: $psql_actmedia !!");
				purswayLogs("Data pushed Failed Trough PSQL ! ERROR :: $psql_actmedia \n");
                	}
		}
		else{
			purswayMailError("ERROR :: Project :: $all_project_flag File Type :: $p_type ","UTF-8 File conversion Failed for $p_type !!");
			purswayLogs("UTF-8 File conversion Failed !\n");
		}
	}
}
##########################################################################################################


sub actMediaPsqlDistinctExecution{
                my ($psql_dtype,$psql_dproject) = @_;

		my $actMediatmptableName_d = actMediaTempTableName($psql_dtype,$psql_dproject);
                my $actMediTableName_d = actMediTableNameProject($psql_dtype,$psql_dproject);
                my $psqldout = `psql -h dmp.pursway.com -U postgres  -d postgres -p 5432 -c \"insert INTO um_schema.$actMediTableName_d select distinct * from um_schema.$actMediatmptableName_d"`;
		#$psqldout = `psql -h localhost -U postgres  -d postgres -p 5432 -c \"insert INTO um_schema.$actMediTableName_d select distinct * from um_schema.$actMediatmptableName_d"`;
                if($? ==0){
			purswayLogUpdate($dbh,$logInsertId,'is_psql_distinct','1');
			purswayLogs("$psql_dtype Distinct PSQL Execution Successfull for  $psql_dproject  !\n");
	 	}else{
			purswayMailError("ERROR :: Project :: $psql_dproject File Type :: $psql_dtype "," $psql_dtype Distinct PSQL Execution Failed, ERROR :: $psqldout!!");
			purswayLogs("$psql_dtype Distinct PSQL Execution Failed Error :: $psqldout !\n");		
		}
}

sub actMediaPsqlTruncateExecution{
                my ($psql_Trtype,$psql_Trproject) = @_;

                my $actMediatmptableName_tr = actMediaTempTableName($psql_Trtype,$psql_Trproject);
                my $psqlout_tr = `psql -h dmp.pursway.com -U postgres  -d postgres -p 5432 -c \"truncate table um_schema.$actMediatmptableName_tr"`;
		if($? ==0){
			purswayLogUpdate($dbh,$logInsertId,'is_psql_truncate','1');
                        purswayLogs("$psql_Trtype Truncate PSQL Execution Successfull for  $psql_Trproject  !\n");
                }else{
			purswayMailError("ERROR :: Project :: $psql_Trproject File Type :: $psql_Trtype "," $psql_Trtype Truncate PSQL Execution Failed ERROR :: $psqlout_tr!!");
                        purswayLogs("$psql_Trtype Truncate PSQL Execution Failed Error :: $psqlout_tr !\n");
                }
}               


sub actMediaPsqlExecution{
		my ($psql_type,$psql_project) = @_;
		my $psqlPrjectPath = actMediapsqlProjectPath($psql_type,$psql_project);
		my $actMediatmptableName = actMediaTempTableName($psql_type,$psql_project);
                #$psqlout = `psql -h localhost -U postgres -d postgres -p 5432 -c \"copy um_schema.mediadata_pure_activities from '/home/xavient/analysis/act_new_updated.txt' WITH DELIMITER E'\t'"`;
               my  $psqlout = `psql -h dmp.pursway.com -U postgres -d postgres -p 5432 -c \"\\copy um_schema.$actMediatmptableName from '$psqlPrjectPath' WITH DELIMITER E'\t'"`;
		return $?;
}

sub actMediaUTFConversion{
	my($UtfPath,$utfOutFileName,$utf_type)	= 	@_;
	purswayLogs("Coverting file format to UTF-8 !\n");
	if($utf_type eq 'act'){
	       `iconv -f ISO-8859-1 -t UTF-8 $UtfPath/$utfOutFileName >> $UtfPath/new_act_updated`;
	}else{
	       `iconv -f ISO-8859-1 -t UTF-8 $UtfPath/$utfOutFileName >> $UtfPath/mediafile_updated_new`;
	}
	return $?;
}

########################## Find Last file Name from Local Directory For Running Process####################

sub lastFileName {
	my($fpath,$falias,$process,$proFlag) = @_;
		if($falias eq 'act_' && $proFlag eq 'all'){
			purswayLogs("===============================================================================");
			purswayLogs("Start Time For All process ::$time\n");
		}
		purswayLogs("=============================Process Started For $process Files====================\n\n");
		purswayLogs("Start Time For $process Files Process ====================================== $time\n");
		my $ls_file =  `cd $fpath; ls -Art $falias*.gz | sort |tail -n 1`;
		if($ls_file){
			return $ls_file;
		}
		else{
			return 0;
		}
}
##########################################################################################################

################################# check FTP file exist ###################################################

sub checkFileListExist{
	my ($file_name,$source,$fileLocaltion) = @_;
		
	if($source =~ m/^s3/g){
		return `s3cmd ls -ArT $source/$file_name`;
	}
	elsif($source =~ m/^gs/g){
		return `gsutil ls -Art $source/$file_name`;
	}
	elsif($source =~ m/^ftp/g){
	 	my $ftp = Net::FTP->new(Host=>$host, Debug => 0, Passive=>1)or die "Cannot connect to some.host.name: $@";
         	$ftp->login($user,$pass)or die "Cannot login ", $ftp->message;
                $ftp->cwd($dir)or die "Cannot change working directory ", $ftp->message;
                $ftp->binary;
                return $ftp->ls("$file_name");
	}
	elsif($source =~ m/^sftp/g){
		return	sftpFileExist($sftp_dir,$fileLocaltion,$file_name);
	}
	else{  
		purswayLogs("Source value not Found To check files !\n");
	}
}
##########################################################################################################

#####################################file Download and Uncompress#########################################

sub FileListDownload{
	my ($file_name,$source,$fpath,$type) = @_;

	if($source =~ m/^s3/g){
		 	my $fmove = localFileMovement($fpath,$type);
			purswayLogUpdate($dbh,$logInsertId,'is_file_moved','1');
		 	s3Gsutilfilecopy($file_name,$source,$fpath,$type,'S3');
	}
	elsif($source =~ m/^gs/g){
		 	my $fmove1 = localFileMovement($fpath,$type);
			purswayLogUpdate($dbh,$logInsertId,'is_file_moved','1');
                        s3Gsutilfilecopy($file_name,$source,$fpath,$type,'gsutil');
        }
	elsif($source =~ m/^ftp/g){
			 my $ftpm = localFileMovement($fpath,$type);
			purswayLogUpdate($dbh,$logInsertId,'is_file_moved','1');
		 	ftpDownload($file_name,$type,$fpath);
        }
	elsif($source =~ m/^sftp/g){	
			my $sftpm = localFileMovement($fpath,$type);
			purswayLogUpdate($dbh,$logInsertId,'is_file_moved','1');
			sftpDownload($sftp_dir,$fpath,$file_name);		
	}
	else{
			purswayLogs("Source value not Found To Download files !\n");
	}
}
##########################################################################################################

############################ Function For FTP Download ###################################################
sub ftpDownload {
	my($ftpFile,$ftpType,$local_file_path) 	= @_;
		 my $ftpd = Net::FTP->new(Host=>$host, Debug => 0, Passive=>1)or die "Cannot connect to some.host.name: $@";
                 $ftpd->login($user,$pass)or die "Cannot login ", $ftpd->message;
                 $ftpd->cwd($dir)or die "Cannot change working directory ", $ftpd->message;
                 $ftpd->binary;
                 my @f_list     =  $ftpd->ls("$ftpFile");
		 purswayLogs("Files Downloaded and Processing will Start in 2 Seconds !\n");
		 purswayLogUpdate($dbh,$logInsertId,'download_source','FTP');
                 sleep(2);
                 foreach my $file(@f_list){
			 purswayLogs("Processing $ftpType File Downloading From FTP =====> Name :: $file !\n");
                         $ftpd->get($file)or die "get failed ", $ftpd->message;
                         print FILELIST  "$file|$time\n";
                         #print  "$file|$time\n";
                         `mv $file $local_file_path`;
                 }
                 $ftpd->quit;
		 purswayLogUpdate($dbh,$logInsertId,'is_file_downloaded','1');
}
#########################################################################################################

##################################### Function For File Uncompress ######################################
sub fileUncompress {
	my($uncompFilepath,$uncomptype)	= @_;
		purswayLogs("Uncompressing the Downloaded files present in folder !\n");
                my $uncomp_file = $uncomptype."_*";
                if($uncomptype eq  'meta'){
                        return system("cd $uncompFilepath; tar zxvf $uncomp_file");
                }
                else{
                        return  system("cd $uncompFilepath; gzip -d $uncomp_file");
                }
}
#########################################################################################################

################################## Function For Local File Movement for Process #########################
sub localFileMovement{
	my($file_path,$ftype) = @_;
		 purswayLogs("Removing all Existing Files from $ftype  !\n"); 
		 my $s3move = system("rm $file_path/$ftype* ");
                 if($s3move ==0){
			purswayLogs("Old Files Removed from $ftype Successfully !\n");
			return 1;
		 }else{
			purswayMailError("ERROR :: File Type :: $ftype "," No $ftype Files Found to remove in Folder !!");
			purswayLogs("No Files Found to remove in Folder !\n");
			return 0;
		 }
}

#########################################################################################################

##################################  Function For Download FIle From S3 Or Gsutil Server #################
sub s3Gsutilfilecopy{
	 my($filename,$fsource,$file_path1,$ftype1,$sourceType) = @_;
	 my $s3Out;
		purswayLogs("Files copy from $sourceType to $ftype1 local folder\n");
		if($sourceType eq 'S3'){
			purswayLogUpdate($dbh,$logInsertId,'download_source','S3');
                        $s3Out = system("s3cmd cp $fsource/$filename $file_path1");
                }
                else{
			purswayLogUpdate($dbh,$logInsertId,'download_source','GSUTIL');
                        $s3Out = system("gsutil cp $fsource/$filename $file_path1");
                }
                if($s3Out==0){
			purswayLogUpdate($dbh,$logInsertId,'is_file_downloaded','1');
			purswayLogs("Files copied from $sourceType Successfully !\n");
                }
		else{
			purswayLogs("Files copying Failed from $sourceType  !\n");
                }
}
##########################################################################################################

################################## Find Next date after finding last file from local directory ###########
sub FindNextDate{
	my $l_file  = shift;
	my @lname = split('_',$l_file);
        my @ldate = split('\.',$lname[1]);
	my @tdate = split('',$ldate[0]);	
	my $entered_date = "$tdate[0]$tdate[1]$tdate[2]$tdate[3]-$tdate[4]$tdate[5]-$tdate[6]$tdate[7]";
	my $date = Time::Piece->strptime($entered_date, "%Y-%m-%d");
	$date += ONE_DAY;
	return  join('',split('-',$date->strftime("%Y-%m-%d")));
}
###########################################################################################################

#################################### Function For SFTP DownLoad ###########################################
sub sftpDownload {
        my($remoteDir,$local_dir,$fileName) = @_;
	my $sfFileName1 = originalFileName($fileName);
        my $sftp = Net::SFTP::Foreign->new (host => $sftp_host, timeout => 240,user => $sftp_user, password => $sftp_pass, autodie => 1);
        if ( $sftp->error ) {
	     purswayLogs("Error while creating object of SFTP Foreign:: $sftp->error\n");
        }
        else{
	     purswayLogUpdate($dbh,$logInsertId,'download_source','SFTP');
             $sftp->setcwd($remoteDir);
             $sftp->get("$sfFileName1", "$local_dir/$sfFileName1");
             if ($sftp->error){
			purswayMailError("ERROR :: File Type ::$sfFileName1 ","Unable to download file \"$sfFileName1\" : " . $sftp->error . "");
			purswayLogs("Unable to download file \"$sfFileName1\" : " . $sftp->error . "\n\n");
             }
             else{
			purswayLogUpdate($dbh,$logInsertId,'is_file_downloaded','1');
			purswayLogs("file Downloaded :$sfFileName1\n");
             }
        }
}
###########################################################################################################

############################## Function For Check SFTP File Exist ########################################
sub sftpFileExist {
        my($remoteDir,$localDir,$OfileName) = @_;
        my $orFileName = originalFileName($OfileName);
        my $sftp = Net::SFTP::Foreign->new (host => $sftp_host, timeout => 240,user => $sftp_user, password => $sftp_pass, autodie => 1);
        if ( $sftp->error ) {
	     purswayMailError("ERROR :: File Type :: $orFileName ","Error while creating object of SFTP Foreign:: $sftp->error\n");
	     purswayLogs("Error while creating object of SFTP Foreign:: $sftp->error\n");
        }
        else{
             $sftp->setcwd($remoteDir);
             my $list_sftp = $sftp->ls(".", names_only => 1,no_wanted => qr/^\./);
             my @fileexist = grep(/$orFileName/,@$list_sftp);
             return @fileexist;
        }
}
############################################################################################################

################################## Function For Execution All Meta PSQL Command ############################
sub metaPSQLExecution {
        my($FILEVAR)    = shift;
        my $meta_psql_status = 0;
	my $meta_command_string;
        while(<$FILEVAR>){
                chomp $_;
                my $meta_psql = `$_`;
                if($? ==0)
                {
			$meta_psql_status = 1;
			purswayLogs("Meta PSQL Executed Successfully ::$_ \n");
                }
                else{
                        $meta_psql_status = 2;
			purswayMailError("ERROR :: File Type :: Meta ","Meta Data Push Failed through PSQL in Old Table for :: $_ Error :: $meta_psql");
			purswayLogs("Meta PSQL Execution Failed :: $_ Error :: $meta_psql\n");
                }
        }
	purswayLogUpdate($dbh,$logInsertId,'is_psql_executed','1')if($meta_psql_status==1);
	purswayLogUpdate($dbh,$logInsertId,'is_meta_psql_failed','1')if($meta_psql_status==2);
	return 1;
}
#############################################################################################################

######################################### Function For Log File #############################################
sub purswayLogs{
	my($logMessage) =	shift;
	print LOGHANDLER  "$logMessage";
	print  "$logMessage";
}
close LOGHANDLER;
close FILELIST;
#############################################################################################################

####################################### Insert Function For Logs In Table ####################################
sub purswayLogInsert {
	my ($dbhlogs,$iProcess,$project_name)	=@_;
	my $logInserttime = sprintf("%04d-%02d-%02d %02d:%02d:%02d", $year+1900, $mon+1, $mday, $hour, $min, $sec);
	my $logInsert = qq{insert into SCA_pursway_logs (process_type,project_name,created_date) values (?, ?, ?)};
        my $sthlogs = $dbhlogs->prepare($logInsert);
        $sthlogs->execute($iProcess,$project_name,$logInserttime);
	$sthlogs->finish;
	return ($dbhlogs->{mysql_insertid});
	}
##############################################################################################################

###################################### Update Function For Logs In Table #####################################
sub purswayLogUpdate{
	 my ($dbhupdate,$logId,$column_name,$col_value)   = @_;
	 my $logUpdatetime = sprintf("%04d-%02d-%02d %02d:%02d:%02d", $year+1900, $mon+1, $mday, $hour, $min, $sec);
	 my $logupdate = qq{update SCA_pursway_logs set $column_name = '$col_value',updated_date = '$logUpdatetime' where id = '$logId'};
	 $dbhupdate->do($logupdate);
}

sub checkFileExecuteStatus{
	my($file_date,$fileProcess,$fileproject)	= @_;
	my $condition;
	$condition = "and is_psql_executed ='1' and is_psql_truncate ='1' and is_psql_distinct ='1'" if ($fileProcess eq 'act' || $fileProcess eq 'media');
	$condition = "and is_psql_executed ='1' and is_psql_truncate ='1' and is_psql_distinct ='1' and is_meta_psql_failed ='0' and is_meta_psql_truncate_failed ='0' and is_meta_psql_distinct_failed ='0'" if ($fileProcess eq 'meta'); 
	
	my $ch_file_name  = $fileProcess.'_'.$file_date;
	my $check_query = qq{select count(*) from SCA_pursway_logs where new_filename  like '$ch_file_name%' and project_name ='$fileproject' $condition};
	my $check_sth =  $dbh->prepare($check_query);
        $check_sth->execute();
        my $check_count = $check_sth->fetchrow_array();
	return $check_count;
}
################################################################################################################

############################## Function For Finding Original File Anme #####################################
sub originalFileName {
        my($s_fileName) = shift;
        $s_fileName =~ s/\*//;
        if($s_fileName =~m/^act/){
                $s_fileName = $s_fileName.".txt.gz";
        }
        elsif($s_fileName =~m/^media/){
                $s_fileName = $s_fileName.".txt_00.gz";
        }
        else{
                $s_fileName = $s_fileName.".tar.gz";
        }
        return $s_fileName;
}
############################################################################################################

sub purswayActLocalDirectory {
	my($act_projectName)	=	shift;
	my $activity_Dir;
	if($act_projectName eq 'SCA'){
		$activity_Dir	= '/hadoop/sony/SCA/act';	
	}elsif($act_projectName eq 'SME'){
		$activity_Dir   = '/hadoop/sony/SME/act';
	}elsif($act_projectName eq 'SEL'){
		$activity_Dir   = '/hadoop/sony/SEL/act';
	}elsif($act_projectName eq 'SNEI'){
                $activity_Dir   = '/hadoop/sony/SNEI/act';
        }elsif($act_projectName eq 'SPE'){
                $activity_Dir   = '/hadoop/sony/SPE/act';
        }elsif($act_projectName eq 'SPHE'){
                $activity_Dir   = '/hadoop/sony/SPHE/act';
        }else{
		print "Project Name Not matched for ACT Local Directory!\n";
		exit;
	}
	return $activity_Dir;
}

sub purswayMediaLocalDirectory{
	my($media_projectName)	=	shift;
	my $media_Dir;
	if($media_projectName eq 'SCA'){
                $media_Dir   = '/hadoop/sony/SCA/media';
        }elsif($media_projectName eq 'SME'){
                $media_Dir   = '/hadoop/sony/SME/media';
        }elsif($media_projectName eq 'SEL'){
                $media_Dir   = '/hadoop/sony/SEL/media';
        }elsif($media_projectName eq 'SNEI'){
                $media_Dir   = '/hadoop/sony/SNEI/media';
        }elsif($media_projectName eq 'SPE'){
                $media_Dir   = '/hadoop/sony/SPE/media';
        }elsif($media_projectName eq 'SPHE'){
                $media_Dir   = '/hadoop/sony/SPHE/media';
        }else{
                print "Project Name Not matched for Media Local Directory!\n";
                exit;
        }
        return $media_Dir;
}

sub purswayMetaLocalDirectory{
	my($meta_projectName) =       shift;
	my $meta_Dir;
        if($meta_projectName eq 'SCA'){
                $meta_Dir   = '/hadoop/sony/SCA/meta/metafiles';
        }elsif($meta_projectName eq 'SME'){
                $meta_Dir   = '/hadoop/sony/SME/meta/metafiles';
        }elsif($meta_projectName eq 'SEL'){
                $meta_Dir   = '/hadoop/sony/SEL/meta/metafiles';
        }elsif($meta_projectName eq 'SNEI'){
                $meta_Dir   = '/hadoop/sony/SNEI/meta/metafiles';
        }elsif($meta_projectName eq 'SPE'){
                $meta_Dir   = '/hadoop/sony/SPE/meta/metafiles';
        }elsif($meta_projectName eq 'SPHE'){
                $meta_Dir   = '/hadoop/sony/SPHE/meta/metafiles';
        }else{
                print "Project Name Not matched for Meta Local Directory!\n";
                exit;
        }
        return $meta_Dir;
}

sub metaMergeShellScript{
	my($meta_Project_name)	= shift;
	my $meta_script_name;
	if($meta_Project_name eq 'SCA'){
                $meta_script_name   = '/hadoop/sony/SCA/meta/metaprocessor.sh';
        }elsif($meta_Project_name eq 'SME'){
                $meta_script_name   = '/hadoop/sony/SME/meta/metaprocessor.sh';
        }elsif($meta_Project_name eq 'SEL'){
                $meta_script_name   = '/hadoop/sony/SEL/meta/metaprocessor.sh';
        }elsif($meta_Project_name eq 'SNEI'){
                $meta_script_name   = '/hadoop/sony/SNEI/meta/metaprocessor.sh';
        }elsif($meta_Project_name eq 'SPE'){
                $meta_script_name   = '/hadoop/sony/SPE/meta/metaprocessor.sh';
        }elsif($meta_Project_name eq 'SPHE'){
                $meta_script_name   = '/hadoop/sony/SPHE/meta/metaprocessor.sh';
        }else{
                print "Project Name Not matched for Meta Merge Script!\n";
                exit;
        }
        return $meta_script_name;
}

sub actFileDownloadSource{
	my($act_project_source)	= shift;
	my $act_Source_Name;
	if($act_project_source eq 'SCA'){
                $act_Source_Name   = 'sftp://199.38.164.171';
        }elsif($act_project_source eq 'SME'){
                $act_Source_Name   = 'sftp://199.38.164.171';
        }elsif($act_project_source eq 'SEL'){
                $act_Source_Name   = 'sftp://199.38.164.171';
        }elsif($act_project_source eq 'SNEI'){
                $act_Source_Name   = 'sftp://199.38.164.171';
        }elsif($act_project_source eq 'SPE'){
                $act_Source_Name   = 'sftp://199.38.164.171';
        }elsif($act_project_source eq 'SPHE'){
                $act_Source_Name   = 'sftp://199.38.164.171';
        }else{
                print "Project Source Name Not Defined for ACT Process !\n";
                exit;
        }
        return $act_Source_Name;

}

sub mediaFileDownloadSource{
	my($media_project_source) = shift;
	my $media_Source_Name;
	if($media_project_source eq 'SCA'){
                $media_Source_Name = 'sftp://199.38.164.171';
        }elsif($media_project_source eq 'SME'){
                $media_Source_Name = 'sftp://199.38.164.171';
        }elsif($media_project_source eq 'SEL'){
                $media_Source_Name = 'sftp://199.38.164.171';
        }elsif($media_project_source eq 'SNEI'){
                $media_Source_Name = 'sftp://199.38.164.171';
        }elsif($media_project_source eq 'SPE'){
                $media_Source_Name = 'sftp://199.38.164.171';
        }elsif($media_project_source eq 'SPHE'){
                $media_Source_Name = 'sftp://199.38.164.171';
        }else{
                print "Project Source Name Not Defined for Media Process !\n";
                exit;
        }
        return $media_Source_Name;
}

sub metaFileDownloadSource{
	my($meta_project_source) = shift;
	my $meta_Source_Name;
	if($meta_project_source eq 'SCA'){
                $meta_Source_Name = 'sftp://199.38.164.171';
        }elsif($meta_project_source eq 'SME'){
                $meta_Source_Name = 'sftp://199.38.164.171';
        }elsif($meta_project_source eq 'SEL'){
                $meta_Source_Name = 'sftp://199.38.164.171';
        }elsif($meta_project_source eq 'SNEI'){
                $meta_Source_Name = 'sftp://199.38.164.171';
        }elsif($meta_project_source eq 'SPE'){
                $meta_Source_Name = 'sftp://199.38.164.171';
        }elsif($meta_project_source eq 'SPHE'){
                $meta_Source_Name = 'sftp://199.38.164.171';
        }else{
                print "Project Source Name Not Defined for Meta Process !\n";
                exit;
        }
        return $meta_Source_Name;
}

sub sftpSourceDirPath{
	my($sftp_dir_project)	= shift;
	my $sftp_dir_path;
	if($sftp_dir_project eq 'SCA'){
                $sftp_dir_path = '/sca/logs';
        }elsif($sftp_dir_project eq 'SME'){
                $sftp_dir_path = '/sca/SME';
        }elsif($sftp_dir_project eq 'SEL'){
                $sftp_dir_path = '/sca/SEL';
        }elsif($sftp_dir_project eq 'SNEI'){
                $sftp_dir_path = '/sca/SNEI';
        }elsif($sftp_dir_project eq 'SPE'){
                $sftp_dir_path = '/sca/SPE';
        }elsif($sftp_dir_project eq 'SPHE'){
                $sftp_dir_path = '/sca/SPHE';
        }else{
                print "SFTP Source Directory Path Not Defined !\n";
                exit;
        }
        return $sftp_dir_path;
}

sub ftpSourceDirPath{
        my($ftpdir_project)   = shift;
        my $ftpdir_path;
        if($ftpdir_project eq 'SCA'){
                $ftpdir_path = '/pursway';
        }elsif($ftpdir_project eq 'SME'){
                $ftpdir_path = '/pursway';
        }elsif($ftpdir_project eq 'SEL'){
                $ftpdir_path = '/pursway';
        }elsif($ftpdir_project eq 'SNEI'){
                $ftpdir_path = '/pursway';
        }elsif($ftpdir_project eq 'SPE'){
                $ftpdir_path = '/pursway';
        }elsif($ftpdir_project eq 'SPHE'){
                $ftpdir_path = '/pursway';
        }else{
                print "FTP Source Directory Path Not Defined !\n";
                exit;
        }
        return $ftpdir_path;
}

sub checkCommandLineArgument {
	my(@ARGMENT)	=	@_;
	if(scalar @ARGMENT >= 2){
        	if(!exists $project_hash->{$ARGMENT[1]}){
                        print "Project Name is Not defined Correctly, Please provide Correct Project Name in Argument!!\n";
                        exit;
        	}
        	$logfile = "purswaylogs/$project_hash->{$ARGMENT[1]}_ACT_logs_$now.logs" if($ARGMENT[0] eq 'act');
        	$logfile = "purswaylogs/$project_hash->{$ARGMENT[1]}_MEDIA_logs_$now.logs" if($ARGMENT[0] eq 'media');
        	$logfile = "purswaylogs/$project_hash->{$ARGMENT[1]}_META_logs_$now.logs" if($ARGMENT[0] eq 'meta');
        	$logfile = "purswaylogs/$project_hash->{$ARGMENT[1]}_ALL_logs_$now.logs" if($ARGMENT[0] eq 'all');
	}
	else{
         	print "\nPlease Pass Two Required Argument 1 -> Process Name , 2 -> Project Name !!\n";
         	print "usage:\n1: Pass the Argument which Project & Process want to Run \neg. perl sriptname arg1* arg2* arg3 arg4\nARG1* : Process Name(act -> Act Process, media -> Media Process, meta -> Meta Process,all -> All Process)\nARG2* : Project Name(sca -> SCA,mse -> SME,sel -> SEL,snei -> SNEI,spe -> SPE,sphe -> SPHE)\nARG3  : Date -: if you want to run for a specific date then pass [yyyy-mm-dd] format\nARG4 : -F to Download Forcefully any process file\n\n";
        	exit;
	}
}

sub actMediapsqlProjectPath{
        my($psql_process_type,$psql_projectName)        = @_;
        my $psql_path_string;
        if($psql_process_type eq 'act'){
                if($psql_projectName eq 'SCA'){
                        $psql_path_string   = '/hadoop/sony/SCA/act/new_act_updated';
                }elsif($psql_projectName eq 'SME'){
                        $psql_path_string   = '/hadoop/sony/SME/act/new_act_updated';
                }elsif($psql_projectName eq 'SEL'){
                        $psql_path_string   = '/hadoop/sony/SEL/act/new_act_updated';
                }elsif($psql_projectName eq 'SNEI'){
                        $psql_path_string   = '/hadoop/sony/SNEI/act/new_act_updated';
                }elsif($psql_projectName eq 'SPE'){
                        $psql_path_string   = '/hadoop/sony/SPE/act/new_act_updated';
                }elsif($psql_projectName eq 'SPHE'){
                        $psql_path_string   = '/hadoop/sony/SPHE/act/new_act_updated';
                }else{
                        print "Project Name Not matched for ACT File!\n";
                        exit;
                }
        }
        else{
                if($psql_projectName eq 'SCA'){
                        $psql_path_string   = '/hadoop/sony/SCA/media/mediafile_updated_new';
                }elsif($psql_projectName eq 'SME'){
                        $psql_path_string   = '/hadoop/sony/SME/media/mediafile_updated_new';
                }elsif($psql_projectName eq 'SEL'){
                        $psql_path_string   = '/hadoop/sony/SEL/media/mediafile_updated_new';
                }elsif($psql_projectName eq 'SNEI'){
                        $psql_path_string   = '/hadoop/sony/SNEI/media/mediafile_updated_new';
                }elsif($psql_projectName eq 'SPE'){
                        $psql_path_string   = '/hadoop/sony/SPE/media/mediafile_updated_new';
                }elsif($psql_projectName eq 'SPHE'){
                        $psql_path_string   = '/hadoop/sony/SPHE/media/mediafile_updated_new';
                }else{
                        print "Project Name Not matched for Media File!\n";
                        exit;
                }
        }
        return $psql_path_string;
}
 
sub actMediTableNameProject {
	my($psqlTable_Process_Name,$psqlTable_projectName)        = @_;
        my $psql_table_string_name;
        if($psqlTable_Process_Name eq 'act'){
                if($psqlTable_projectName eq 'SCA'){
                        $psql_table_string_name   = 'mediadata_pure_activities';
                }elsif($psqlTable_projectName eq 'SME'){
                        $psql_table_string_name   = 'mediadata_pure_activities_sme';
                }elsif($psqlTable_projectName eq 'SEL'){
                        $psql_table_string_name   = 'mediadata_pure_activities_sel';
                }elsif($psqlTable_projectName eq 'SNEI'){
                        $psql_table_string_name   = 'mediadata_pure_activities_snei';
                }elsif($psqlTable_projectName eq 'SPE'){
                        $psql_table_string_name   = 'mediadata_pure_activities_spe';
                }elsif($psqlTable_projectName eq 'SPHE'){
                        $psql_table_string_name   = 'mediadata_pure_activities_sphe';
                }else{
                        print "Project Name Not matched for ACT File!\n";
                        exit;
                }
        }
        else{
                if($psqlTable_projectName eq 'SCA'){
                        $psql_table_string_name   = 'mediadata_impressions';
                }elsif($psqlTable_projectName eq 'SME'){
                        $psql_table_string_name   = 'mediadata_impressions_sme';
                }elsif($psqlTable_projectName eq 'SEL'){
                        $psql_table_string_name   = 'mediadata_impressions_sel';
                }elsif($psqlTable_projectName eq 'SNEI'){
                        $psql_table_string_name   = 'mediadata_impressions_snei';
                }elsif($psqlTable_projectName eq 'SPE'){
                        $psql_table_string_name   = 'mediadata_impressions_spe';
                }elsif($psqlTable_projectName eq 'SPHE'){
                        $psql_table_string_name   = 'mediadata_impressions_sphe';
                }else{
                        print "Project Name Not matched for Media File!\n";
                        exit;
                }
        }
        return $psql_table_string_name;
}

sub actMediaTempTableName {
        my($psqlTempTable_ProcessName,$psqlTempTable_projectName)        = @_;
        my $psql_temp_table_string_name;
        if($psqlTempTable_ProcessName eq 'act'){
                if($psqlTempTable_projectName eq 'SCA'){
                        $psql_temp_table_string_name   = 'mediadata_pure_activities_sca_tmp';
                }elsif($psqlTempTable_projectName eq 'SME'){
                        $psql_temp_table_string_name   = 'mediadata_pure_activities_sme_tmp';
                }elsif($psqlTempTable_projectName eq 'SEL'){
                        $psql_temp_table_string_name   = 'mediadata_pure_activities_sel_tmp';
                }elsif($psqlTempTable_projectName eq 'SNEI'){
                        $psql_temp_table_string_name   = 'mediadata_pure_activities_snei_tmp';
                }elsif($psqlTempTable_projectName eq 'SPE'){
                        $psql_temp_table_string_name   = 'mediadata_pure_activities_spe_tmp';
                }elsif($psqlTempTable_projectName eq 'SPHE'){
                        $psql_temp_table_string_name   = 'mediadata_pure_activities_sphe_tmp';
                }else{
                        print "Project Name Not matched for ACT Table Name!\n";
                        exit;
                }
        }
        else{
                if($psqlTempTable_projectName eq 'SCA'){
                        $psql_temp_table_string_name   = 'mediadata_impressions_sca_tmp';
                }elsif($psqlTempTable_projectName eq 'SME'){
                        $psql_temp_table_string_name   = 'mediadata_impressions_sme_tmp';
                }elsif($psqlTempTable_projectName eq 'SEL'){
                        $psql_temp_table_string_name   = 'mediadata_impressions_sel_tmp';
                }elsif($psqlTempTable_projectName eq 'SNEI'){
                        $psql_temp_table_string_name   = 'mediadata_impressions_snei_tmp';
                }elsif($psqlTempTable_projectName eq 'SPE'){
                        $psql_temp_table_string_name   = 'mediadata_impressions_spe_tmp';
                }elsif($psqlTempTable_projectName eq 'SPHE'){
                        $psql_temp_table_string_name   = 'mediadata_impressions_sphe_tmp';
                }else{
                        print "Project Name Not matched for Media Table Name!\n";
                        exit;
                }
        }
        return $psql_temp_table_string_name;
}

sub metaPSQLTruncate{
        my ($meta_truncate_project)     = @_;
        my $meta_truncate_filename;
        if($meta_truncate_project eq 'SCA'){
                $meta_truncate_filename = 'metaFile/SCA_metaPSQLFileTruncateList.txt';
        }elsif($meta_truncate_project eq 'SME'){
                $meta_truncate_filename = 'metaFile/SME_metaPSQLFileTruncateList.txt';
        }elsif($meta_truncate_project eq 'SEL'){
                $meta_truncate_filename = 'metaFile/SEL_metaPSQLFileTruncateList.txt';
        }elsif($meta_truncate_project eq 'SNEI'){
                $meta_truncate_filename = 'metaFile/SNEI_metaPSQLFileTruncateList.txt';
        }elsif($meta_truncate_project eq 'SPE'){
                $meta_truncate_filename = 'metaFile/SPE_metaPSQLFileTruncateList.txt';
        }elsif($meta_truncate_project eq 'SPHE'){
                $meta_truncate_filename = 'metaFile/SPHE_metaPSQLFileTruncateList.txt';
        }
        else{
                print "Project Source Name Not Defined for Meta Process !\n";
                exit;
        }
        open(FILETRUNCATE,"$meta_truncate_filename") || die "unable to open $!";
	my $meta_truncate_status = 0;
        while(<FILETRUNCATE>){
                chomp $_;
                `$_`;
                if($?==0){
			$meta_truncate_status = 1;
                        purswayLogs("Meta PSQL  Truncate Executed Successfully ::$_ \n");
                }
                else{
			$meta_truncate_status = 2;
			purswayMailError("ERROR :: File Type ::$meta_truncate_filename","Meta PSQL Truncate Execution Failed ::$_  ERROR :: $?!!");
                        purswayLogs("Meta PSQL Truncate Execution Failed ::$_ \n");
                }
        }
	purswayLogUpdate($dbh,$logInsertId,'is_psql_truncate','1')if($meta_truncate_status==1);
        purswayLogUpdate($dbh,$logInsertId,'is_meta_psql_truncate_failed','1')if($meta_truncate_status==2);
        close FILETRUNCATE;
}

sub metaPSQLInsert{
        my ($meta_insert_project)     = @_;
        my $meta_insert_filename;
        if($meta_insert_project eq 'SCA'){
               $meta_insert_filename = 'metaFile/SCA_metaPSQLFileInsertList.txt';
        }elsif($meta_insert_project eq 'SME'){
               $meta_insert_filename = 'metaFile/SME_metaPSQLFileInsertList.txt';
        }elsif($meta_insert_project eq 'SEL'){
               $meta_insert_filename = 'metaFile/SEL_metaPSQLFileInsertList.txt';
        }elsif($meta_insert_project eq 'SNEI'){
               $meta_insert_filename = 'metaFile/SNEI_metaPSQLFileInsertList.txt';
        }elsif($meta_insert_project eq 'SPE'){
               $meta_insert_filename = 'metaFile/SPE_metaPSQLFileInsertList.txt';
        }elsif($meta_insert_project eq 'SPHE'){
               $meta_insert_filename = 'metaFile/SPHE_metaPSQLFileInsertList.txt';
        }
        else{
                print "Project Source Name Not Defined for Meta Process !\n";
                exit;
        }
        open(FILEINSERT,"$meta_insert_filename") || die "unable to open $!";
	my $meta_insert_status = 0;
        while(<FILEINSERT>){
                chomp $_;
                `$_`;
                if($?==0){
			$meta_insert_status = 1;
                        purswayLogs("Meta PSQL Insert Executed Successfully ::$_ \n");
                }
                else{
			$meta_insert_status = 2;
			purswayMailError("ERROR :: File Type :: $meta_insert_filename","Meta PSQL Insert Execution Failed ::$_  ERROR :: $?");
                        purswayLogs("Meta PSQL Insert Execution Failed ::$_  ERROR :: $?\n");
                }
        }
	purswayLogUpdate($dbh,$logInsertId,'is_psql_distinct','1')if($meta_insert_status==1);
        purswayLogUpdate($dbh,$logInsertId,'is_meta_psql_distinct_failed','1')if($meta_insert_status==2);
        close FILEINSERT;
}

sub metaPSQLCommand{
	my ($meta_old_project)     = @_;
        my $meta_old_filename;
        if($meta_old_project eq 'SCA'){
               $meta_old_filename = 'metaFile/SCA_metaPSQLFileList.txt';
        }elsif($meta_old_project eq 'SME'){
               $meta_old_filename = 'metaFile/SME_metaPSQLFileList.txt';
        }elsif($meta_old_project eq 'SEL'){
               $meta_old_filename = 'metaFile/SEL_metaPSQLFileList.txt';
        }elsif($meta_old_project eq 'SNEI'){
               $meta_old_filename = 'metaFile/SNEI_metaPSQLFileList.txt';
        }elsif($meta_old_project eq 'SPE'){
               $meta_old_filename = 'metaFile/SPE_metaPSQLFileList.txt';
        }elsif($meta_old_project eq 'SPHE'){
               $meta_old_filename = 'metaFile/SPHE_metaPSQLFileList.txt';
        }
        else{
                print "Project Source Name Not Defined for Meta Process !\n";
                exit;
        }
	return $meta_old_filename;

}

sub purswayMailError {
	my($error_sub,$error_message) = @_;

	my $smtpserver = 'email-smtp.us-east-1.amazonaws.com';
        my $smtpport = 465;
        my $smtpuser   = 'AKIAIAGDQF4ZXAFFFZBA';
        my $smtppassword = 'AreI0EV+2/f36WiG/Js5/+v84z3xdLyVmnzXSogNcyxP';

        my $smtp = Net::SMTP::SSL->new($smtpserver, Port=>$smtpport,Debug => 0,) or die "Connection Failed To Server\n";

        $smtp->datasend ("AUTH LOGIN\n");
        $smtp->response();
        $smtp->datasend (encode_base64($smtpuser)); #username
        $smtp->response();
        $smtp->datasend (encode_base64($smtppassword)); # password
        $smtp->response();
        $smtp->mail('sysadmin@map-global.com');
        $smtp->to("gmarya\@xavient.com\n");
	$smtp->to("vsrivastava1\@xavient.com\n");
	$smtp->to("mkinamdar\@xavient.com\n");
        $smtp->data();
        $smtp->datasend("To: gmarya\@xavient.com,vsrivastava1\@xavient.com,mkinamdar\@xavient.com\n");
        $smtp->datasend("From: sysadmin\@map-global.com\n");
        $smtp->datasend("Subject: $error_sub");
        $smtp->datasend("\n");
        # Send the body.
        $smtp->datasend("--*BCKTR*\n");
        $smtp->datasend("Content-Type: text/plain\n\n");
        $smtp->datasend("$error_message\n");
        $smtp->datasend("\n");
        $smtp->datasend();
        if($smtp->datasend)
        {
                $smtp->quit();
                print "Mail Sent Successfull for Pursway Execution Error !!\n";
        }
        else
        {
                my $err_msg = $smtp->message();
                $smtp->quit();
                print "error message mail failed ==$err_msg\n";
        }
}
$dbh->disconnect;
