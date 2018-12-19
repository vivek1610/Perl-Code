use warnings;
use strict;
use Data::Dumper;

open('PROBELIST','C:\Users\vsrivas3\Desktop\BSM\BSM Report\probelist.txt') or die "Could not creat file ' $!";


#my $Probelist;
my @alertProbelist;
my @NotInAlertProbelist;

while(<PROBELIST>){
	chomp($_);
	$_	=~ s/^\s+|\s+$//g ;
	my $probename = $_;
	#print ("probe name ---$probename\n");
	open('ALERTLIST','C:\Users\vsrivas3\Desktop\BSM\BSM Report\alertlist.txt') or die "Could not creat file ' $!";
	
	push(@NotInAlertProbelist,$probename);	
	
	while(<ALERTLIST>){
		chomp($_);
		$_	=~ s/^\s+|\s+$//g ;
		my $alertname = $_;		
		if($alertname  =~ /\Q$probename\E/){	
				#print ("Probe in alert----$probename\n");
				push(@alertProbelist,$probename);
				pop(@NotInAlertProbelist);				
				last;
		}else{				
				#$Probelist->{$probename} = {'ALERT'=>'NONE', 'NOALERT'=>$probename};
				next;
		}
	}	
	close ALERTLIST;
}

#print("alert probe list ----@alertProbelist\n");
#print("not in alert probe list ----@NotInAlertProbelist\n");
#print Dumper(%$Probelist);

open('ALERTPROBE','>C:\Users\vsrivas3\Desktop\BSM\BSM Report\AlertingProbeList.txt') or die "Could not creat file ' $!";
open('NOALERTPROBE','>C:\Users\vsrivas3\Desktop\BSM\BSM Report\NONAlertingProbeList.txt') or die "Could not creat file ' $!";

foreach my $Aprobename (@alertProbelist) {
	print  ALERTPROBE "$Aprobename\n";
}
foreach my $Nalertprobename (@NotInAlertProbelist) {
	print  NOALERTPROBE "$Nalertprobename\n";
}
close PROBELIST;

