#!/usr/bin/perl
use strict;
use warnings;
use Data::Dumper;

my $date = localtime();
my @date_array 	= split(' ',$date); 
#my $dt = $date_array[2].'_'.$date_array[1].'_'.$date_array[4].'_'.$date_array[3];
my $dt = $date_array[2].'_'.$date_array[1].'_'.$date_array[4];
my $tm = $date_array[3];
my $stoplogfile  = "Log_Stop_$dt.txt";
open(STLOG,">>$stoplogfile");
if (-e $stoplogfile){
	open(STLOG,">>$stoplogfile");
}else{
	open(STLOG,">$stoplogfile");
}

print "\n Select any service from below (Enter corresponding number)\n\n";
print "\t\t1. Press For ESR GW / ESR SMART GUI\n \t\t2. Press For All The Adapters\n \t\t3. Press For BPEL-11G \n \t\t4. Press For HTML UNIT \n \t\t5. Press For BASIC / ICP / PEP / LIQUID OFFICE / DDF \n \t\t6. Press For IMM\n \t\t7. Press For SOA / TWC-SOA [for PROD at 12:00 AM SOA GUIS / SOAPS,  after 1:00 AM all the SOA GWS] \n \t\t8. Press For All Process\n \t\t9. Press For EXIT\n\n";
START:
chomp (my $input=<>);
my $instanceName = {
			'1'=>'ESR GW / ESR SMART GUI',
			'2'=>'All The Adapters',
			'3'=>'BPEL-11G',
			'4'=>'HTML UNIT',
			'5'=>'BASIC / ICP / PEP / LIQUID OFFICE / DDF',
			'6'=>'IMM',
			'7'=>'SOA / TWC-SOA [for PROD at 12:00 AM SOA GUIS / SOAPS,  after 1:00 AM all the SOA GWS]',
			'8'=>'For ALL'
		  };

if($input =~ m/[1-8]/){
		my $file = 'serverlist.txt';
		open('SERVER',$file) || die "Could not open $file: $!";
		my @filevar = <SERVER>;
		chomp @filevar;
 		my $hashvar = {};
		my $inputVal;
		
	
		foreach my $val (@filevar)
		{	
			next if($val =~ m/^\#/);
        		my ($Index,$serverIp,$userName,$command) = split('\t',$val);
	      		push(@{$hashvar->{$Index}},{'SERVER'=>$serverIp,'USER'=>$userName,'COMMAND'=>$command});
 		}
 		if($input =~ m/[1-7]/){
			bpelHtmlService($input)if ($input == 3||$input == 4);			
 			$inputVal	= $hashvar->{$input};
				
                	print STLOG "LOG Started for Single Process=====================================Time - $dt $tm\n\n";
			print STLOG  "Stop Process Begin for $instanceName->{$input} \n\n";
                	foreach my $sval (@$inputVal){
				my $output = `ssh  $sval->{USER}\@$sval->{SERVER} "$sval->{COMMAND}"`;
				print "$output\n";
				print STLOG "$output\n";
			
			}
	                print STLOG "LOG Closed for Single Process======================================Time - $dt $tm\n\n\n";
			print STLOG "=================================================================================================================\n\n\n";
		}else{
                        print STLOG "LOG Started for ALL Process=========================================Time - $dt $tm\n\n";
			print STLOG  "Stop Process Begin for $instanceName->{$input} \n\n";
			foreach my $index (1..7){
			$inputVal    	= $hashvar->{$index};
				bpelHtmlService($index) if ($index == 3 ||$index == 4);
				print STLOG  "Stop Process Started for $instanceName->{$index} \n\n";
				foreach my $allval (@$inputVal){
					my $output = `ssh  $allval->{USER}\@$allval->{SERVER} "$allval->{COMMAND}"`;
					print "$output\n";
					print STLOG "$output\n";

                        	}
			sleep(1);
			
			}
			print STLOG "LOG Closed for ALL Process===========================================Time - $dt $tm\n\n";
			print STLOG "=================================================================================================================\n\n\n";
		}
	}
	elsif($input =~ m/[9]/){
		exit;
	}
	else
	{
		print "Please enter valid input\n";
		goto START;
	}

close STLOG;
sub bpelHtmlService {

	my($userInput)	= shift;
	if($userInput == 3){
                 print "Please Stoped BPEL-11G GUI Services First !\n";
		 print "If BPEL-11G GUI Services Already Stoped Press (y) else Press (n) !== ";
                 my $bpel = <STDIN>;
                 chomp($bpel);
                 print "Please enter valid input\n" if($bpel =~ m/n/);
                 goto START if($bpel =~ m/n/);

	}
	elsif($userInput == 4){
                 print "Please Stoped HMML UNIT GUI Services First !\n";
                 print "If HMML UNIT GUI Services Already Stoped Press (y) else Press (n) !== ";
                 my $htmlunit = <STDIN>;
                 chomp($htmlunit);
                 print "Please enter valid input\n" if($htmlunit =~ m/n/);
                 goto START if($htmlunit =~ m/n/);
	}

}







