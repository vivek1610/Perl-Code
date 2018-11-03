#!/usr/bin/perl
use strict;
use warnings;
use DBI;
use DBD::Oracle;
use Term::ANSIColor qw(:constants);
########========= DB Details =========########

my $dsn = "DBI:Oracle:host=10.5.3.183;sid=xe;port=1521";
my $userid = "PERL_CON";
my $password = "PERL123";
##########live
#my $dsn = "DBI:Oracle:host=10.61.2.90;sid=nfclct;port=1521";
#my $userid = "chgtwy_test";
#my $password = "dwarf5"

########======== DB COnnection =======########

my $dbh = DBI->connect($dsn, $userid, $password ) or die "Couldn't connect to database: " . DBI->errstr;
chomp (my $DTSENT = `date +%m-%d-%Y-%I%M%p`);
chomp (my $current_date_time = `date +%m-%d-%Y-%r`);
chomp (my $current_date = `date +%m_%d_%Y`);
my ($CustomerIdentifier,$Supplier,$Action,$Flag,$log_file,$PON);
my $process_hash = {
                        'lsr'   	=>      'LSR_ORDER',
                        'csr'   	=>      'CSR_ORDER',
                        'av'   		=>      'AV'
                   }; 
checkCommandLineArgument(@ARGV);
my $DDD = lsrFutureDate()if($process_hash->{$ARGV[0]} eq 'LSR_ORDER');

print "==================Process Start for $process_hash->{$ARGV[0]}=======================\n\n";

########========== Input File =======#########
my $inputfile 		= processInputFileName($process_hash->{$ARGV[0]});
my $header_file 	= processHeaderfile($process_hash->{$ARGV[0]});
my $request_file 	= processRequestfile($process_hash->{$ARGV[0]});
my $property_file 	= processPropertiesfile($process_hash->{$ARGV[0]});
my $pon_table_name      = ponTableName($process_hash->{$ARGV[0]});
my $pon_column_name 	= ponColumnName($process_hash->{$ARGV[0]});

open (LOGFILE,">>$log_file") || die "Can't find file:$log_file for $process_hash->{$ARGV[0]} Process, $!";
open (INFILE,"$inputfile") || die "Can't find file:$inputfile for $process_hash->{$ARGV[0]} Process, $!";

my $cnt = 0;
while (<INFILE>)
{
	chomp;
	++$cnt && next if($cnt==0);
	chomp(my $NUM = `date +%m%d%y%M%S`);
	chomp($PON = "NSRTST".$NUM);
	my @row = split(/\t/,$_);
	$CustomerIdentifier = $row[0];
	$Supplier = $row[1];
	$Action = $row[2];
	$Flag = $row[3];
	if (uc $Flag eq "YES")
	{
		print "\n\nCustomer --> $CustomerIdentifier\nSupplier --> $Supplier\nAction --> $Action\n\n";
	}
	else
	{
		next;
	}

	print LOGFILE "\n\nProcess started at $current_date_time for $process_hash->{$ARGV[0]} Process\n";
	print LOGFILE "Date Sent Value = $DTSENT for $process_hash->{$ARGV[0]} Process\n";
	print LOGFILE "Future Date DDD = $DDD for $process_hash->{$ARGV[0]} Process\n" if($process_hash->{$ARGV[0]} eq 'LSR_ORDER') ;

########==== PON Validate from DB ====########
PON:
	my $query = qq{select count(*) from $pon_table_name where $pon_column_name ='$PON'};
	my $sth =  $dbh->prepare($query);
	$sth->execute();
	my $size = $sth->fetchrow_array();
	print LOGFILE "PON value = $PON\n";
if ($size >= 1)
{
	print "This PON is already exist in DB\n";
	print LOGFILE "This PON is already exist in DB\n";
	++$NUM;
	$PON = "NSRTST".$NUM;
	print "Retry with new PON:$PON\n";
	print LOGFILE "Retry with new PON\n";
	goto PON;
}
else
{
	print LOGFILE "CustomerId = $CustomerIdentifier\n";
	print LOGFILE "Supplier Value = $Supplier\n";
	my $InterfaceVersion;

	if ($Supplier =~m/^ATT$|^ATTSE$|^QWEST$|^CenturyLink$/)
	{
		$InterfaceVersion = "LSOG10";
		print LOGFILE "Interface Version = $InterfaceVersion\n";
	}
	elsif ($Supplier =~m/^VZE$|^VZW$|^Frontier$|^FPC$/)
	{
		$InterfaceVersion = "LSOG9";
		print LOGFILE "Interface Version = $InterfaceVersion\n";
	}else{
		$InterfaceVersion = "LSOG6";
                print LOGFILE "Interface Version = $InterfaceVersion\n";
	}		

	if (lc $Action =~m/^validate$|^save$|^submit$/i)
	{
		print LOGFILE "Action Value = $Action for $process_hash->{$ARGV[0]} Process\n";
		system(qq{sed -i -e '/CustomerIdentifier/ s/\".*\"/"$CustomerIdentifier"/g' -e '/Supplier/ s/\".*\"/"$Supplier"/g' -e '/InterfaceVersion/ s/\".*\"/"$InterfaceVersion"/g' -e '/Action/ s/\".*\"/"$Action"/g' $header_file});
		print YELLOW, "\nBelow is the modified $header_file file for $process_hash->{$ARGV[0]} Process:\n\n", RESET;
		system ("cat $header_file");
		system(qq{sed -i -e '/PON/ s/\".*\"/"$PON"/g' -e '/DTSENT/ s/\".*\"/"$DTSENT"/g' -e '/DDD/ s/\".*\"/"$DDD"/g' $request_file})if($process_hash->{$ARGV[0]} eq 'LSR_ORDER');
		system(qq{sed -i -e '/TXNUM/ s/\".*\"/"$PON"/g' -e '/DTSENT/ s/\".*\"/"$DTSENT"/g' $request_file})if($process_hash->{$ARGV[0]} eq 'CSR_ORDER' || $process_hash->{$ARGV[0]} eq 'AV');
		print YELLOW, "Below is the modified $request_file file for $process_hash->{$ARGV[0]} Process:\n\n", RESET;
		system ("cat $request_file");
		if ($Action =~m/^submit$/i)
		{
			system(qq{sed -i '/method/ s/processSync/processAsync/g' $property_file});
			print LOGFILE "Process = processAsync\n";
			print YELLOW, "\nBelow is the modified $property_file file for $process_hash->{$ARGV[0]} Process:\n\n", RESET;
			system ("cat $property_file");
		}
		else
		{
			system(qq{sed -i '/method/ s/processAsync/processSync/g' $property_file});
			print LOGFILE "Process = processSync\n";
			print YELLOW, "\nBelow is the modified $property_file file for $process_hash->{$ARGV[0]} Process:\n\n", RESET;
                        system ("cat $property_file");
		}
	}
	print YELLOW, "\nCall - API\n", RESET;
}
$sth->finish();
sleep 1;
}
$dbh->disconnect;
close (INFILE);
close (LOGFILE);

################################ FUNCTION FOR ALL PROCESS LSR,CSR and AV ################################

sub ponColumnName{
	my ($proces_ponName) =    shift;
        my $poncolumn_name;
        if($proces_ponName eq 'CSR_ORDER'){
                $poncolumn_name      = 'inqnum';
        }elsif($proces_ponName eq 'LSR_ORDER' || $proces_ponName eq 'AV'){
                $poncolumn_name      = 'pon';
        }else{
                print "Process Name Not Found for any Pon Column Name:\n";
                exit;
        }
        return $poncolumn_name;

}

sub ponTableName{
	my ($proces_table) =    shift;
        my $table_name;
        if($proces_table eq 'LSR_ORDER'){
                $table_name      = 'lsr_message';
        }elsif($proces_table eq 'CSR_ORDER'){
                $table_name      = 'csr_message';
        }elsif($proces_table eq 'AV'){
                $table_name      = 'supplier_avq_message';
        }else{
                print "Process Name Not Found for any Table Name:\n";
                exit;
        }
        return $table_name;
}

sub processInputFileName{
        my ($process_name) =    shift;
        my $file_name;
        if($process_name eq 'LSR_ORDER'){
                $file_name 	= 'lsr_input.txt';
        }elsif($process_name eq 'CSR_ORDER'){
                $file_name 	= 'csr_input.txt';
        }elsif($process_name eq 'AV'){
                $file_name 	= 'av_input.txt';
        }else{
                print "Process Name Not Found for any Input Files:\n";
                exit;
        }
        return $file_name;
}

sub processHeaderfile {
	my ($header_name) =    shift;
        my $header_file_name;
        if($header_name eq 'LSR_ORDER'){
                $header_file_name 	= '../xml/PTEC/lsr_header.xml';
        }elsif($header_name eq 'CSR_ORDER'){
                $header_file_name 	= '../xml/PTEC/csrheader.xml';
        }elsif($header_name eq 'AV'){
                $header_file_name 	= '../xml/PTEC/av_header.xml';
        }else{
                print "Process Name Not Found for any header Files:\n";
                exit;
        }
        return $header_file_name;
}

sub processRequestfile {
	my ($request_name) =    shift;
        my $request_file_name;
        if($request_name eq 'LSR_ORDER'){
                $request_file_name 	= '../xml/PTEC/lsr_request.xml';
        }elsif($request_name eq 'CSR_ORDER'){
                $request_file_name 	= '../xml/PTEC/csrrequest.xml';
        }elsif($request_name eq 'AV'){
                $request_file_name 	= '../xml/PTEC/av_request.xml';
        }else{
                print "Process Name Not Found for any Request Files:\n";
                exit;
        }
        return $request_file_name;
}

sub processPropertiesfile {
	my ($property_name) =    shift;
        my $property_file_name;
        if($property_name eq 'LSR_ORDER'){
                $property_file_name 	= '../xml/PTEC/lsr_request.properties';
        }elsif($property_name eq 'CSR_ORDER'){
                $property_file_name 	= '../xml/PTEC/csr_request.properties';
        }elsif($property_name eq 'AV'){
                $property_file_name 	= '../xml/PTEC/av_request.properties';
        }else{
                print "Process Name Not Found for any Properties Files:\n";
                exit;
        }
        return $property_file_name;	
}


sub lsrFutureDate {
	
	my $futureDate;
	print "Do we have holiday tomorrow (yes/no):\n";
	chomp(my $holiday = <STDIN>);
	if (uc $holiday eq "YES")
	{
        	RETRY:
                	print "Enter the future date in number of days (except 1 -> [2...9]):\n";
                	chomp(my $days = <STDIN>);
                	if ($days =~/^\d{1}$/ && $days =~/^[2-9]$/)
                	{
                        	chomp($futureDate = `date --date='$days day' +%m-%d-%Y`);
                	}
                	else
                	{
                        	print "OOPS! Wrong input retry please\n";
                        	goto RETRY;
                	}
	}
	elsif (uc $holiday eq "NO")
	{
        	chomp($futureDate = `date --date='1 day' +%m-%d-%Y`);
	}
	else
	{
        	print "OOPS! Wrong input retry please\n";
        	exit;
	}
	return $futureDate;
}

sub checkCommandLineArgument {
        my(@ARGMENT)    =       @_;
        if(scalar @ARGMENT == 1){
                if(!exists $process_hash->{$ARGMENT[0]}){
                        print "Process Name is Not defined Correctly, Please provide Correct Process Name in Argument!!\n";
                        exit;
                }
		$log_file = "LSR_Logs/LSR_log_$current_date.log" if($process_hash->{$ARGMENT[0]} eq 'LSR_ORDER');
		$log_file = "CSR_Logs/CSR_log_$current_date.log" if($process_hash->{$ARGMENT[0]} eq 'CSR_ORDER');
		$log_file = "AV_Logs/AV_log_$current_date.log" if($process_hash->{$ARGMENT[0]} eq 'AV');
        }
        else{
                print "\nPlease Pass One Required Argument 1 -> PROCESS NAME !!\n";
                print "usage:\nPass the Argument which Process want to Run \neg. perl sriptname arg1* \n\nARG1* \n1 - Process Name(lsr -> LSR_ORDER Type Process ,csr -> CSR_ORDER Type Process , av -> AV Type Process )\n";
                exit;
        }
}
