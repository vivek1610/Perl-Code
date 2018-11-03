#!/usr/bin/perl
use warnings;
use strict;
use Data::Dumper;
use Net::SFTP::Foreign;
use autodie;
#my $sftp = Net::SFTP::Foreign->new('localhost', user => "xavient", password => "1@34567b", port => "");
my $filename = 'act_20151211*';
my $sftp_dir = '/home/sca/';
my $local_dir = '/home/xavient/vivek';
my $sftp_host = "localhost";
my $sftp_user = "sca";
my $sftp_pass = "k33p1tr\@3l";


open(FILEVAR,'metaPSQLFileList.txt') || die "unable to open $!";
metaPSQLExecution('FILEVAR');

sub metaPSQLExecution {
	my($FILEVAR)	= shift;
	my $meta_psql_status = 0;
	my $meta_string;
	while(<$FILEVAR>){
		chomp $_;
		my $metaOPsql = 0;		
		if($metaOPsql ==0)
		{       $meta_psql_status =1;	
			$meta_string.= "$_ =>$meta_psql_status|";
			#print "$_\n";
		}
		else{
			 $meta_psql_status =2;
			 $meta_string.= "$_ =>$meta_psql_status|";;
			 #print "$_\n";
		}
	}
	print "$meta_string\n";
	return $meta_psql_status;
}

=head


sftpDownload($sftp_dir,$local_dir,$filename);

my $isExist  = sftpFileExist($sftp_dir,$local_dir,$filename);
print "$isExist\n";

sub sftpFileExist {
	my($remoteDir,$localDir,$OfileName) = @_;
	$OfileName = originalFileName($OfileName);
	print "$OfileName====================full file name\n";
        my $sftp = Net::SFTP::Foreign->new (host => $sftp_host, timeout => 240,user => $sftp_user, password => $sftp_pass, autodie => 1);
        if ( $sftp->error ) {
             print "Error while creating object of SFTP Foreign:: $sftp->error\n";
        }
	else{
             $sftp->setcwd($remoteDir);
             my $list_sftp = $sftp->ls(".", names_only => 1,no_wanted => qr/^\./);
	     my @fileexist = grep(/$OfileName/,@$list_sftp);
	     print "@fileexist===================list of file\n";
	     return scalar @fileexist
        }
}

sub originalFileName {
	my($fileName) = shift;
	$fileName =~ s/\*//;
        if($fileName =~m/^act/){
                $fileName = $fileName.".txt.gz";
        }
        elsif($fileName =~m/^media/){
                $fileName = $fileName.".txt_00.gz";
        }
        else{
                $fileName = $fileName.".tar.gz";
        }
	return $fileName;
}


sub sftpDownload {
	my($remoteDir,$localDir,$fileName) = @_;
	$fileName = originalFileName($fileName);
	print "$fileName====================download full file name\n";
	my $sftp = Net::SFTP::Foreign->new (host => $sftp_host, timeout => 240,user => $sftp_user, password => $sftp_pass, autodie => 1);
	if ( $sftp->error ) {
             print "Error while creating object of SFTP Foreign:: $sftp->error\n";
	}
	else{
	     	$sftp->setcwd($remoteDir);
		$sftp->get("$fileName", "$local_dir/$fileName");
		if ($sftp->error){
			print "Unable to download file \"$fileName\" : " . $sftp->error . "\n\n";
		}
		else{
			print "file Downloaded :$fileName\n";
		}
 	}
}
=cut

