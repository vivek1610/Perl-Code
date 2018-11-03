#!/usr/bin/perl
use warnings;
use strict;
use DBI;
use Spreadsheet::ParseExcel;
use Time::Piece;
use Data::Dumper;

my $date = localtime();
my @date_array  = split(' ',$date);
my $dt = $date_array[2].'_'.$date_array[1].'_'.$date_array[4];
my $tm = $date_array[3];

my $driver = "mysql";
my $database = "Alert";
my $dsn = "DBI:$driver:database=$database";
my $userid = "root";
my $password = "root";
my $dbh = DBI->connect($dsn, $userid, $password ) or die $DBI::errstr;
my $st_time  = '08';
my $end_time = '20';
START:
print "\n Select Any Option For Creating Report!!\n\n";
print "\t\t1. Press 1 For Xav BPO Report\n \t\t2. Press 2 For Xav Tier 2 Report\n \t\t3. Press 3 For Xav App Support Report \n\t\t4. Press 4 For Exit\n\n";
chomp (my $input=<>);
my $reportType = {
                        '1'=>'xav_bpo',
                        '2'=>'xav_tier2',
                        '3'=>'xav_app',
			'4'=>'Exit'
                   };
if($input =~ m/[1-3]/){
print "Process started for $reportType->{$input}\n\n";
my $reportName = "$reportType->{$input}_Open_Ack_Resolve_$dt.txt";
#print "$reportName\n";
open(FILEVAR,'NuestarExcelTicket.txt')|| die "Unable to locate file, $!";
open (FILENEW,">$reportName");
print FILENEW  "Case Number\t Priority\t Created By\tCompany\tCase Open Time\tCase Acknowlege Time\tCase Resolve Time\t Ack Time\tResolve Time\tComment\n";

my $hash;
my $cnt = 0;
	while(<FILEVAR>){
		++$cnt && next if($cnt == 0);
		chomp ;
		#print "$_\n";
		my($time_cr,$time_res) =  ('','');
		my @arrayData = split(/\t/,$_);
		if($arrayData[4] =~ m/$reportType->{$input}/){
			#print"$_\n";
			$hash->{$arrayData[0]} = $arrayData[0];
		}
		#if (grep { $arrayData[2]  eq $_ } @nameList) {
		#	$hash->{$arrayData[0]} = $arrayData[0];
		#}
        }
#print Dumper($hash);exit;

foreach (keys %$hash){
chomp $_;
my $idName = $_;
my $selectquery = qq{select * from neustarTicket where ticket_no ='$idName' order by created_date};
#print "query==========$selectquery\n";
my $sth = $dbh->prepare($selectquery);
$sth->execute();
my $countrow =  $sth->rows;
my $comments = 'NA';
if($countrow ==1){
	#print "count  row ==$countrow\n";
	my @data_row =  $sth->fetchrow_array();	
	my $ack_diff = datedifference($data_row[6],$data_row[4]);
	my $resolve_time = '-';
	my $ack_time_1 = $data_row[4];

	if($reportType->{$input} =~ m/xav_tier2/ || $reportType->{$input} =~ m/xav_bpo/){
		$comments  = CommentsBpoTier2('ack',$ack_time_1,$comments);
		
	}
	my $res_diff = 0;
	if($data_row[8] ==1){
		$ack_time_1 = $data_row[6];
		$ack_diff = 0;
		$res_diff = datedifference($data_row[6],$data_row[4]);
		$resolve_time = $data_row[4];
		if($reportType->{$input} =~ m/xav_tier2/ || $reportType->{$input} =~ m/xav_bpo/){
                	$comments  = CommentsBpoTier2('res',$resolve_time,$comments);
		}
	}
	print FILENEW  "$data_row[1]\t$data_row[2]\t$data_row[3]\t$data_row[5]\t$data_row[6]\t$ack_time_1\t$resolve_time\t$ack_diff min\t$res_diff min\t$comments\n";
	#print   "$data_row[1]\t$data_row[2]\t$data_row[3]\t$data_row[5]\t$data_row[6]\t$ack_time_1\t$resolve_time\t$ack_diff min\t$res_diff min\t$comments\n";
}
else{
	print "row count-----------$countrow\n";
	my $diff = 0;
	my $array_data;
	while (my @allData = $sth->fetchrow_array()){
		$allData[-1] =~ s/\r//g;
		push (@$array_data,[@allData]);
	}

 	my $i;
 	my($open_date,$ack_date,$res_date);

 	for ($i=0;$i<=$countrow-1;$i++){
		$comments = 'NA';	
		my($priority,$username,$comp,$all_res_diff,$all_ack_diff);	
		if($i == 0 && $array_data->[$i][5] =~ m/$reportType->{$input}/){
	
			$open_date = $array_data->[0][6];
			$ack_date = $array_data->[$i][4];
			$all_ack_diff =  datedifference($open_date,$ack_date);
			my $z=$i;
			my $cnt_check = 0;
			while($array_data->[$z][5] =~ m/$reportType->{$input}/)
			{
				$res_date = $array_data->[$z][4];
				$priority = $array_data->[$z][2]; 
				$username = $array_data->[$z][3];
				$comp 	  = $array_data->[$z][5];
				$cnt_check++;
				$z++;
			}
			$ack_date = $open_date if($cnt_check == 1);
			$all_ack_diff = 0 if($cnt_check == 1);
			$all_res_diff = datedifference($ack_date,$res_date);
			$i = $z-1;
			if($reportType->{$input} =~ m/xav_tier2/ || $reportType->{$input} =~ m/xav_bpo/){
                        	$comments  = CommentsBpoTier2('ack',$ack_date,$comments);
                	}
			if($reportType->{$input} =~ m/xav_tier2/ || $reportType->{$input} =~ m/xav_bpo/){
                                $comments  = CommentsBpoTier2('res',$res_date,$comments);
                        }
			print FILENEW "$idName\t$priority\t$username\t$comp\t$open_date\t$ack_date\t$res_date\t$all_ack_diff min \t$all_res_diff min\t$comments\n";			
			#print  "$idName\t$priority\t$username\t$comp\t$open_date\t$ack_date\t$res_date\t$all_ack_diff min \t$all_res_diff min\t$comments\n";	
		}
		else{
			$comments = 'NA';
   			if($array_data->[$i][5] !~ m/$reportType->{$input}/){
				$open_date =  $array_data->[$i][4];
			}
			else{
                        	$ack_date = $array_data->[$i][4];
				$all_ack_diff =  datedifference($open_date,$ack_date);
				
                        	my $z=$i;
				my $cnt_check = 0;
                        	while($array_data->[$z][5] eq $reportType->{$input})
                        	{
                                	$res_date = $array_data->[$z][4];
					$priority = $array_data->[$z][2];
                                	$username = $array_data->[$z][3];
                                	$comp     = $array_data->[$z][5];
                                	$z++;
					$cnt_check++;
                        	}
	                        $ack_date = $open_date if($cnt_check == 1);
				$all_ack_diff = 0 if($cnt_check == 1);
				$all_res_diff = datedifference($ack_date,$res_date);
			 	$i = $z-1;
				if($reportType->{$input} =~ m/xav_tier2/ || $reportType->{$input} =~ m/xav_bpo/){
                                	$comments  = CommentsBpoTier2('ack',$ack_date,$comments);
                        	}
                        	if($reportType->{$input} =~ m/xav_tier2/ || $reportType->{$input} =~ m/xav_bpo/){
                                	$comments  = CommentsBpoTier2('res',$res_date,$comments);
                        	}
				print FILENEW  "$idName\t$priority\t$username\t$comp\t$open_date\t$ack_date\t$res_date\t$all_ack_diff min \t$all_res_diff min\t$comments\n";
				#print  "$idName\t$priority\t$username\t$comp\t$open_date\t$ack_date\t$res_date\t$all_ack_diff min \t$all_res_diff min\t$comments\n";

			}
		}
	}
}
}
close FILENEW;
close FILEVAR;
}
elsif($input =~ m/[4]/){
                exit;
        }
else
    {
        print "Please enter valid input\n";
        goto START;
    }


sub datedifference {
        my($st_date,$cl_date)  = @_;

                my $format = '%Y-%m-%d %H:%M:%S';

                my $diff = Time::Piece->strptime($cl_date,$format)- Time::Piece->strptime($st_date,$format);

                return $diff->minutes;
}

sub CommentsBpoTier2 {
	my ($type,$processDate,$pro_coment)   =  @_;
		my $comments1 = $pro_coment;
	 	my($pro_date1,$pro_time1) = split(/ /,$processDate);
                my($pro_hour1,,) = split(/:/,$pro_time1);
		if ($type eq 'ack'){
                	if( $pro_hour1 > $end_time || $pro_hour1 < $st_time){
                        	$comments1 = 'Pass acknowledge because ticket acknowledge 8PM to 8AM';
                	}
		}
		else{
			 if( $pro_hour1 > $end_time || $pro_hour1 < $st_time){
                                $comments1 = 'Pass acknowledge and resolved time because same 8PM to 8AM';
                       	 }
		}
		return $comments1;
}



