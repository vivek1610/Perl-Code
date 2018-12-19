use warnings;
use strict;
use Data::Dumper;
use Date::Calc qw/Delta_Days/;
use Excel::Writer::XLSX;
use POSIX qw(strftime);

#my $datestart1 = strftime "%d%b%Y", localtime;
 
open('FILEIN','C:\Users\vsrivas3\Desktop\Report\Reportinput.txt') or die "Could not creat file ' $!";

#open('FILEOUT','>C:\Users\vsrivas3\Desktop\Report\MonthlyReportOutput.txt') or die "Could not creat file ' $!";

my $UserName = {
				"ST"	=>	"SAURABH THAKUR",
				"MG"	=>	"MAYANK GROVER",
				"AS"	=>	"AKSHAT SINGH",
				"RK"	=>	"RANJAN KUMAR",
				"VSR"	=>	"VIVEK SRIVASTAVA",
				"VS"	=>	"VIKAS SHARMA",
				"SM"	=>	"SHREYANSH MEHRA"
				};
				
my $userhash  = {
				"RANJAN KUMAR"		=>0,
				"SAURABH THAKUR"	=>0,
				"VIVEK SRIVASTAVA"	=>0,
				"SHREYANSH MEHRA"	=>0,
				"AKSHAT SINGH"		=>0,
				"MAYANK GROVER"		=>0,
				"VIKAS SHARMA"		=>0
				};

my @dataArray;
my $hashlist = {
				"Number" 		=> "",
				"State"			=> "",
				"Request_item" 		=> "",
				"Short_description"	=> "",
				"Assigned_to" 		=> "",
				"Assignment_group"	=> "",
				"Actual_start" 		=> "",
				"Actual_end"		=> "",
				"Reference_1" 		=> "",
				"Reference_2"		=> "",
				"Reference_3" 		=> "",
				"Reference_4"		=> "",
				"Reference_5"		=> ""
				};
				
my $month_hash =	{
					'01' 	=> "Jan",
					'02' 	=> "Feb",
					'03' 	=> "Mar",
					'04' 	=> "Apr",
					'05' 	=> "May",
					'06' 	=> "Jun",
					'07' 	=> "Jul",
					'08' 	=> "Aug",
					'09' 	=> "Sep",
					'10' 	=> "Oct",
					'11' 	=> "Nov",
					'12' 	=> "Dec"
					};

while(<FILEIN>){
			chomp($_);
			$_	=~ s/^\s+|\s+$//g;
			next if($_ =~/^Number/g);	
			my @list = split(/\t/,$_);
			#print("each row val =======$list[12] -----$list[8]\n");
			#next if($list[8] eq 'NULL'||$list[9] eq 'NULL'||$list[10] eq 'NULL'||$list[11] eq 'NULL'||$list[12] eq 'NULL');
			$hashlist = {
						"Number" 			=> "$list[0]",
						"State"				=> "$list[1]",
						"Request_item" 		=> "$list[2]",
						"Short_description"	=> "$list[3]",
						"Assigned_to" 		=> "$list[4]",
						"Assignment_group"	=> "$list[5]",
						"Actual_start" 		=> "$list[6]",
						"Actual_end"		=> "$list[7]",
						"Reference_1" 		=> "$list[8]",
						"Reference_2"		=> "$list[9]",
						"Reference_3" 		=> "$list[10]",
						"Reference_4"		=> "$list[11]",
						"Reference_5"		=> "$list[12]"
						};
			push(@dataArray,$hashlist);
}

my ($totalTicketCount,$selfTicketCount,$moniUpliftTicketCount,$escalatedTicketCount,$datediff,$cbaTicketCount) = (0,0,0,0,0,0);

my $hashname;
foreach my $userName (keys %$UserName){
	$hashname->{$UserName->{$userName}} ={'BSM_ADD'=>0,'BSM_UPDATE'=>0,'BSM_SELF'=>0,'SC_EVP'=>0,'SC_ADD'=>0,'SC_UPDATE'=>0,'SC_SELF'=>0,'OVO'=>0,'DYNATRACE'=>0,'DYNATRACE_SELF'=>0};
}

my ($bsmAddCount_SM,$bsmAddCount_MG,$bsmAddCount_AS,$bsmAddCount_RK,$bsmAddCount_VS,$bsmAddCount_VSR,$bsmAddCount_ST) = (0,0,0,0,0,0,0);
my ($bsmUpCount_SM,$bsmUpCount_MG,$bsmUpCount_AS,$bsmUpCount_RK,$bsmUpCount_VS,$bsmUpCount_VSR,$bsmUpCount_ST) = (0,0,0,0,0,0,0);
my ($bsmSelfCount_SM,$bsmSelfCount_MG,$bsmSelfCount_AS,$bsmSelfCount_RK,$bsmSelfCount_VS,$bsmSelfCount_VSR,$bsmSelfCount_ST) = (0,0,0,0,0,0,0);
my ($ScEvpCount_SM,$ScEvpCount_MG,$ScEvpCount_AS,$ScEvpCount_RK,$ScEvpCount_VS,$ScEvpCount_VSR,$ScEvpCount_ST) = (0,0,0,0,0,0,0);
my ($ScAddCount_SM,$ScAddCount_MG,$ScAddCount_AS,$ScAddCount_RK,$ScAddCount_VS,$ScAddCount_VSR,$ScAddCount_ST) = (0,0,0,0,0,0,0);
my ($ScUpCount_SM,$ScUpCount_MG,$ScUpCount_AS,$ScUpCount_RK,$ScUpCount_VS,$ScUpCount_VSR,$ScUpCount_ST) = (0,0,0,0,0,0,0);
my ($ScSelfCount_SM,$ScSelfCount_MG,$ScSelfCount_AS,$ScSelfCount_RK,$ScSelfCount_VS,$ScSelfCount_VSR,$ScSelfCount_ST) = (0,0,0,0,0,0,0);
my ($ovoCount_SM,$ovoCount_MG,$ovoCount_AS,$ovoCount_RK,$ovoCount_VS,$ovoCount_VSR,$ovoCount_ST) = (0,0,0,0,0,0,0);
my ($dyCount_SM,$dyCount_MG,$dyCount_AS,$dyCount_RK,$dyCount_VS,$dyCount_VSR,$dyCount_ST) = (0,0,0,0,0,0,0);
my ($dySelfCount_SM,$dySelfCount_MG,$dySelfCount_AS,$dySelfCount_RK,$dySelfCount_VS,$dySelfCount_VSR,$dySelfCount_ST) = (0,0,0,0,0,0,0);

my ($month_Value,$yearval);

foreach my $rowval (@dataArray) {

	$totalTicketCount++;	
	$userhash->{$rowval->{Assigned_to}}++;
	my ($datediffvalue,$month_Value1,$year1) = dateDifferenceindays($rowval->{Actual_start},$rowval->{Actual_end});
	$month_Value	=	$month_Value1;
	$yearval		=	$year1;
	$datediff = $datediff+$datediffvalue;	
	
	if($rowval->{Reference_5} ne 'NON CBA'){
		$cbaTicketCount = $cbaTicketCount+1;
	}	
	if($rowval->{Reference_3} =~m/^SELF-SERVICE/){
		$selfTicketCount 	= $selfTicketCount+1;	
	}
	if($rowval->{Reference_3} =~m/^MONITORING UPLIFT/){
		$moniUpliftTicketCount 	= $moniUpliftTicketCount+1;	
	}
	if($rowval->{Reference_3} =~m/^ESCALATED HIGH PRIORITY/){
		$escalatedTicketCount 	= $escalatedTicketCount+1;	
	}
	
	if($rowval->{Reference_1} eq 'BSM' && $rowval->{Reference_3} ne 'SELF-SERVICE'){	

		if($rowval->{Reference_2} =~/ADD/){
		
				if($rowval->{Assigned_to} eq $UserName->{SM}){

					$bsmAddCount_SM = $bsmAddCount_SM+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_SM,'BSM_UPDATE'=>$bsmUpCount_SM,'BSM_SELF'=>$bsmSelfCount_SM,'SC_EVP'=>$ScEvpCount_SM,'SC_ADD'=>$ScAddCount_SM,'SC_UPDATE'=>$ScUpCount_SM,'SC_SELF'=>$ScSelfCount_SM,'OVO'=>$ovoCount_SM,'DYNATRACE'=>$dyCount_SM,'DYNATRACE_SELF'=>$dySelfCount_SM};	

				}elsif($rowval->{Assigned_to} eq $UserName->{ST}){
				
					$bsmAddCount_ST = $bsmAddCount_ST+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_ST,'BSM_UPDATE'=>$bsmUpCount_ST,'BSM_SELF'=>$bsmSelfCount_ST,'SC_EVP'=>$ScEvpCount_ST,'SC_ADD'=>$ScAddCount_ST,'SC_UPDATE'=>$ScUpCount_ST,'SC_SELF'=>$ScSelfCount_ST,'OVO'=>$ovoCount_ST,'DYNATRACE'=>$dyCount_ST,'DYNATRACE_SELF'=>$dySelfCount_ST};	
					
				}elsif($rowval->{Assigned_to} eq $UserName->{MG}){
				
					$bsmAddCount_MG = $bsmAddCount_MG+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_MG,'BSM_UPDATE'=>$bsmUpCount_MG,'BSM_SELF'=>$bsmSelfCount_MG,'SC_EVP'=>$ScEvpCount_MG,'SC_ADD'=>$ScAddCount_MG,'SC_UPDATE'=>$ScUpCount_MG,'SC_SELF'=>$ScSelfCount_MG,'OVO'=>$ovoCount_MG,'DYNATRACE'=>$dyCount_MG,'DYNATRACE_SELF'=>$dySelfCount_MG};	
					
				}elsif($rowval->{Assigned_to} eq $UserName->{AS}){	
				
					$bsmAddCount_AS = $bsmAddCount_AS+$rowval->{Reference_4};					
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_AS,'BSM_UPDATE'=>$bsmUpCount_AS,'BSM_SELF'=>$bsmSelfCount_AS,'SC_EVP'=>$ScEvpCount_AS,'SC_ADD'=>$ScAddCount_AS,'SC_UPDATE'=>$ScUpCount_AS,'SC_SELF'=>$ScSelfCount_AS,'OVO'=>$ovoCount_AS,'DYNATRACE'=>$dyCount_AS,'DYNATRACE_SELF'=>$dySelfCount_AS};
					
				}elsif($rowval->{Assigned_to} eq $UserName->{RK}){
				
					$bsmAddCount_RK = $bsmAddCount_RK+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_RK,'BSM_UPDATE'=>$bsmUpCount_RK,'BSM_SELF'=>$bsmSelfCount_RK,'SC_EVP'=>$ScEvpCount_RK,'SC_ADD'=>$ScAddCount_RK,'SC_UPDATE'=>$ScUpCount_RK,'SC_SELF'=>$ScSelfCount_RK,'OVO'=>$ovoCount_RK,'DYNATRACE'=>$dyCount_RK,'DYNATRACE_SELF'=>$dySelfCount_RK};	
					
				}elsif($rowval->{Assigned_to} eq $UserName->{VSR}){
				
					$bsmAddCount_VSR = $bsmAddCount_VSR+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_VSR,'BSM_UPDATE'=>$bsmUpCount_VSR,'BSM_SELF'=>$bsmSelfCount_VSR,'SC_EVP'=>$ScEvpCount_VSR,'SC_ADD'=>$ScAddCount_VSR,'SC_UPDATE'=>$ScUpCount_VSR,'SC_SELF'=>$ScSelfCount_VSR,'OVO'=>$ovoCount_VSR,'DYNATRACE'=>$dyCount_VSR,'DYNATRACE_SELF'=>$dySelfCount_VSR};
					
				}else{
				
					$bsmAddCount_VS = $bsmAddCount_VS+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_VS,'BSM_UPDATE'=>$bsmUpCount_VS,'BSM_SELF'=>$bsmSelfCount_VS,'SC_EVP'=>$ScEvpCount_VS,'SC_ADD'=>$ScAddCount_VS,'SC_UPDATE'=>$ScUpCount_VS,'SC_SELF'=>$ScSelfCount_VS,'OVO'=>$ovoCount_VS,'DYNATRACE'=>$dyCount_VS,'DYNATRACE_SELF'=>$dySelfCount_VS};
				}				
			}
		elsif($rowval->{Reference_2} =~/UPDATE/){
		
				if($rowval->{Assigned_to} eq $UserName->{SM}){
				
					$bsmUpCount_SM = $bsmUpCount_SM+$rowval->{Reference_4};					
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_SM,'BSM_UPDATE'=>$bsmUpCount_SM,'BSM_SELF'=>$bsmSelfCount_SM,'SC_EVP'=>$ScEvpCount_SM,'SC_ADD'=>$ScAddCount_SM,'SC_UPDATE'=>$ScUpCount_SM,'SC_SELF'=>$ScSelfCount_SM,'OVO'=>$ovoCount_SM,'DYNATRACE'=>$dyCount_SM,'DYNATRACE_SELF'=>$dySelfCount_SM};	

				}elsif($rowval->{Assigned_to} eq $UserName->{ST}){
				
					$bsmUpCount_ST = $bsmUpCount_ST+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_ST,'BSM_UPDATE'=>$bsmUpCount_ST,'BSM_SELF'=>$bsmSelfCount_ST,'SC_EVP'=>$ScEvpCount_ST,'SC_ADD'=>$ScAddCount_ST,'SC_UPDATE'=>$ScUpCount_ST,'SC_SELF'=>$ScSelfCount_ST,'OVO'=>$ovoCount_ST,'DYNATRACE'=>$dyCount_ST,'DYNATRACE_SELF'=>$dySelfCount_ST};	
					
				}elsif($rowval->{Assigned_to} eq $UserName->{MG}){
				
					$bsmUpCount_MG = $bsmUpCount_MG+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_MG,'BSM_UPDATE'=>$bsmUpCount_MG,'BSM_SELF'=>$bsmSelfCount_MG,'SC_EVP'=>$ScEvpCount_MG,'SC_ADD'=>$ScAddCount_MG,'SC_UPDATE'=>$ScUpCount_MG,'SC_SELF'=>$ScSelfCount_MG,'OVO'=>$ovoCount_MG,'DYNATRACE'=>$dyCount_MG,'DYNATRACE_SELF'=>$dySelfCount_MG};	
					
				}elsif($rowval->{Assigned_to} eq $UserName->{AS}){
				
				
				
					$bsmUpCount_AS = $bsmUpCount_AS+$rowval->{Reference_4};					
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_AS,'BSM_UPDATE'=>$bsmUpCount_AS,'BSM_SELF'=>$bsmSelfCount_AS,'SC_EVP'=>$ScEvpCount_AS,'SC_ADD'=>$ScAddCount_AS,'SC_UPDATE'=>$ScUpCount_AS,'SC_SELF'=>$ScSelfCount_AS,'OVO'=>$ovoCount_AS,'DYNATRACE'=>$dyCount_AS,'DYNATRACE_SELF'=>$dySelfCount_AS};
					
				}elsif($rowval->{Assigned_to} eq $UserName->{RK}){
				
					$bsmUpCount_RK = $bsmUpCount_RK+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_RK,'BSM_UPDATE'=>$bsmUpCount_RK,'BSM_SELF'=>$bsmSelfCount_RK,'SC_EVP'=>$ScEvpCount_RK,'SC_ADD'=>$ScAddCount_RK,'SC_UPDATE'=>$ScUpCount_RK,'SC_SELF'=>$ScSelfCount_RK,'OVO'=>$ovoCount_RK,'DYNATRACE'=>$dyCount_RK,'DYNATRACE_SELF'=>$dySelfCount_RK};	
					
				}elsif($rowval->{Assigned_to} eq $UserName->{VSR}){
				
					$bsmUpCount_VSR = $bsmUpCount_VSR+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_VSR,'BSM_UPDATE'=>$bsmUpCount_VSR,'BSM_SELF'=>$bsmSelfCount_VSR,'SC_EVP'=>$ScEvpCount_VSR,'SC_ADD'=>$ScAddCount_VSR,'SC_UPDATE'=>$ScUpCount_VSR,'SC_SELF'=>$ScSelfCount_VSR,'OVO'=>$ovoCount_VSR,'DYNATRACE'=>$dyCount_VSR,'DYNATRACE_SELF'=>$dySelfCount_VSR};
					
				}else{
				
					$bsmUpCount_VS = $bsmUpCount_VS+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_VS,'BSM_UPDATE'=>$bsmUpCount_VS,'BSM_SELF'=>$bsmSelfCount_VS,'SC_EVP'=>$ScEvpCount_VS,'SC_ADD'=>$ScAddCount_VS,'SC_UPDATE'=>$ScUpCount_VS,'SC_SELF'=>$ScSelfCount_VS,'OVO'=>$ovoCount_VS,'DYNATRACE'=>$dyCount_VS,'DYNATRACE_SELF'=>$dySelfCount_VS};
				}
		}	
	}elsif($rowval->{Reference_1} eq 'BSM' && $rowval->{Reference_3} eq 'SELF-SERVICE'){
				
				if($rowval->{Assigned_to} eq $UserName->{SM}){

					$bsmSelfCount_SM = $bsmSelfCount_SM+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_SM,'BSM_UPDATE'=>$bsmUpCount_SM,'BSM_SELF'=>$bsmSelfCount_SM,'SC_EVP'=>$ScEvpCount_SM,'SC_ADD'=>$ScAddCount_SM,'SC_UPDATE'=>$ScUpCount_SM,'SC_SELF'=>$ScSelfCount_SM,'OVO'=>$ovoCount_SM,'DYNATRACE'=>$dyCount_SM,'DYNATRACE_SELF'=>$dySelfCount_SM};	

				}elsif($rowval->{Assigned_to} eq $UserName->{ST}){
				
					$bsmSelfCount_ST = $bsmSelfCount_ST+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_ST,'BSM_UPDATE'=>$bsmUpCount_ST,'BSM_SELF'=>$bsmSelfCount_ST,'SC_EVP'=>$ScEvpCount_ST,'SC_ADD'=>$ScAddCount_ST,'SC_UPDATE'=>$ScUpCount_ST,'SC_SELF'=>$ScSelfCount_ST,'OVO'=>$ovoCount_ST,'DYNATRACE'=>$dyCount_ST,'DYNATRACE_SELF'=>$dySelfCount_ST};
					
				}					
				elsif($rowval->{Assigned_to} eq $UserName->{MG}){
				
					$bsmSelfCount_MG = $bsmSelfCount_MG+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_MG,'BSM_UPDATE'=>$bsmUpCount_MG,'BSM_SELF'=>$bsmSelfCount_MG,'SC_EVP'=>$ScEvpCount_MG,'SC_ADD'=>$ScAddCount_MG,'SC_UPDATE'=>$ScUpCount_MG,'SC_SELF'=>$ScSelfCount_MG,'OVO'=>$ovoCount_MG,'DYNATRACE'=>$dyCount_MG,'DYNATRACE_SELF'=>$dySelfCount_MG};	
					
				}elsif($rowval->{Assigned_to} eq $UserName->{AS}){
				
					$bsmSelfCount_AS = $bsmSelfCount_AS+$rowval->{Reference_4};		
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_AS,'BSM_UPDATE'=>$bsmUpCount_AS,'BSM_SELF'=>$bsmSelfCount_AS,'SC_EVP'=>$ScEvpCount_AS,'SC_ADD'=>$ScAddCount_AS,'SC_UPDATE'=>$ScUpCount_AS,'SC_SELF'=>$ScSelfCount_AS,'OVO'=>$ovoCount_AS,'DYNATRACE'=>$dyCount_AS,'DYNATRACE_SELF'=>$dySelfCount_AS};
					
				}elsif($rowval->{Assigned_to} eq $UserName->{RK}){
				
					$bsmSelfCount_RK = $bsmSelfCount_RK+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_RK,'BSM_UPDATE'=>$bsmUpCount_RK,'BSM_SELF'=>$bsmSelfCount_RK,'SC_EVP'=>$ScEvpCount_RK,'SC_ADD'=>$ScAddCount_RK,'SC_UPDATE'=>$ScUpCount_RK,'SC_SELF'=>$ScSelfCount_RK,'OVO'=>$ovoCount_RK,'DYNATRACE'=>$dyCount_RK,'DYNATRACE_SELF'=>$dySelfCount_RK};	
					
				}elsif($rowval->{Assigned_to} eq $UserName->{VSR}){
				
					$bsmSelfCount_VSR = $bsmSelfCount_VSR+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_VSR,'BSM_UPDATE'=>$bsmUpCount_VSR,'BSM_SELF'=>$bsmSelfCount_VSR,'SC_EVP'=>$ScEvpCount_VSR,'SC_ADD'=>$ScAddCount_VSR,'SC_UPDATE'=>$ScUpCount_VSR,'SC_SELF'=>$ScSelfCount_VSR,'OVO'=>$ovoCount_VSR,'DYNATRACE'=>$dyCount_VSR,'DYNATRACE_SELF'=>$dySelfCount_VSR};
					
				}else{
				
					$bsmSelfCount_VS = $bsmSelfCount_VS+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_VS,'BSM_UPDATE'=>$bsmUpCount_VS,'BSM_SELF'=>$bsmSelfCount_VS,'SC_EVP'=>$ScEvpCount_VS,'SC_ADD'=>$ScAddCount_VS,'SC_UPDATE'=>$ScUpCount_VS,'SC_SELF'=>$ScSelfCount_VS,'OVO'=>$ovoCount_VS,'DYNATRACE'=>$dyCount_VS,'DYNATRACE_SELF'=>$dySelfCount_VS};
				}
	
	}elsif($rowval->{Reference_1} eq 'SITESCOPE' && $rowval->{Reference_3} ne 'SELF-SERVICE'){
	
				if($rowval->{Short_description} =~ m/ENTERPRISE VOICE PORTAL/){	
				
						if($rowval->{Reference_2} =~m/ADD|UPDATE/){						
						
						if($rowval->{Assigned_to} eq $UserName->{SM}){

							$ScEvpCount_SM = $ScEvpCount_SM+$rowval->{Reference_4};
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_SM,'BSM_UPDATE'=>$bsmUpCount_SM,'BSM_SELF'=>$bsmSelfCount_SM,'SC_EVP'=>$ScEvpCount_SM,'SC_ADD'=>$ScAddCount_SM,'SC_UPDATE'=>$ScUpCount_SM,'SC_SELF'=>$ScSelfCount_SM,'OVO'=>$ovoCount_SM,'DYNATRACE'=>$dyCount_SM,'DYNATRACE_SELF'=>$dySelfCount_SM};	

						}elsif($rowval->{Assigned_to} eq $UserName->{ST}){
						
							$ScEvpCount_ST = $ScEvpCount_ST+$rowval->{Reference_4};
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_ST,'BSM_UPDATE'=>$bsmUpCount_ST,'BSM_SELF'=>$bsmSelfCount_ST,'SC_EVP'=>$ScEvpCount_ST,'SC_ADD'=>$ScAddCount_ST,'SC_UPDATE'=>$ScUpCount_ST,'SC_SELF'=>$ScSelfCount_ST,'OVO'=>$ovoCount_ST,'DYNATRACE'=>$dyCount_ST,'DYNATRACE_SELF'=>$dySelfCount_ST};							
						}					
						elsif($rowval->{Assigned_to} eq $UserName->{MG}){
						
							$ScEvpCount_MG = $ScEvpCount_MG+$rowval->{Reference_4};
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_MG,'BSM_UPDATE'=>$bsmUpCount_MG,'BSM_SELF'=>$bsmSelfCount_MG,'SC_EVP'=>$ScEvpCount_MG,'SC_ADD'=>$ScAddCount_MG,'SC_UPDATE'=>$ScUpCount_MG,'SC_SELF'=>$ScSelfCount_MG,'OVO'=>$ovoCount_MG,'DYNATRACE'=>$dyCount_MG,'DYNATRACE_SELF'=>$dySelfCount_MG};	
							
						}elsif($rowval->{Assigned_to} eq $UserName->{AS}){
						
							$ScEvpCount_AS = $ScEvpCount_AS+$rowval->{Reference_4};							
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_AS,'BSM_UPDATE'=>$bsmUpCount_AS,'BSM_SELF'=>$bsmSelfCount_AS,'SC_EVP'=>$ScEvpCount_AS,'SC_ADD'=>$ScAddCount_AS,'SC_UPDATE'=>$ScUpCount_AS,'SC_SELF'=>$ScSelfCount_AS,'OVO'=>$ovoCount_AS,'DYNATRACE'=>$dyCount_AS,'DYNATRACE_SELF'=>$dySelfCount_AS};
							
						}elsif($rowval->{Assigned_to} eq $UserName->{RK}){
						
							$ScEvpCount_RK = $ScEvpCount_RK+$rowval->{Reference_4};
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_RK,'BSM_UPDATE'=>$bsmUpCount_RK,'BSM_SELF'=>$bsmSelfCount_RK,'SC_EVP'=>$ScEvpCount_RK,'SC_ADD'=>$ScAddCount_RK,'SC_UPDATE'=>$ScUpCount_RK,'SC_SELF'=>$ScSelfCount_RK,'OVO'=>$ovoCount_RK,'DYNATRACE'=>$dyCount_RK,'DYNATRACE_SELF'=>$dySelfCount_RK};	
							
						}elsif($rowval->{Assigned_to} eq $UserName->{VSR}){
						
							$ScEvpCount_VSR = $ScEvpCount_VSR+$rowval->{Reference_4};
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_VSR,'BSM_UPDATE'=>$bsmUpCount_VSR,'BSM_SELF'=>$bsmSelfCount_VSR,'SC_EVP'=>$ScEvpCount_VSR,'SC_ADD'=>$ScAddCount_VSR,'SC_UPDATE'=>$ScUpCount_VSR,'SC_SELF'=>$ScSelfCount_VSR,'OVO'=>$ovoCount_VSR,'DYNATRACE'=>$dyCount_VSR,'DYNATRACE_SELF'=>$dySelfCount_VSR};
							
						}else{
						
							$ScEvpCount_VS = $ScEvpCount_VS+$rowval->{Reference_4};
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_VS,'BSM_UPDATE'=>$bsmUpCount_VS,'BSM_SELF'=>$bsmSelfCount_VS,'SC_EVP'=>$ScEvpCount_VS,'SC_ADD'=>$ScAddCount_VS,'SC_UPDATE'=>$ScUpCount_VS,'SC_SELF'=>$ScSelfCount_VS,'OVO'=>$ovoCount_VS,'DYNATRACE'=>$dyCount_VS,'DYNATRACE_SELF'=>$dySelfCount_VS};
						}
						
					}
				}else{
					if($rowval->{Reference_2} eq 'ADD'){
						
						if($rowval->{Assigned_to} eq $UserName->{SM}){

							$ScAddCount_SM = $ScAddCount_SM+$rowval->{Reference_4};
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_SM,'BSM_UPDATE'=>$bsmUpCount_SM,'BSM_SELF'=>$bsmSelfCount_SM,'SC_EVP'=>$ScEvpCount_SM,'SC_ADD'=>$ScAddCount_SM,'SC_UPDATE'=>$ScUpCount_SM,'SC_SELF'=>$ScSelfCount_SM,'OVO'=>$ovoCount_SM,'DYNATRACE'=>$dyCount_SM,'DYNATRACE_SELF'=>$dySelfCount_SM};	

						}elsif($rowval->{Assigned_to} eq $UserName->{ST}){
						
							$ScAddCount_ST = $ScAddCount_ST+$rowval->{Reference_4};
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_ST,'BSM_UPDATE'=>$bsmUpCount_ST,'BSM_SELF'=>$bsmSelfCount_ST,'SC_EVP'=>$ScEvpCount_ST,'SC_ADD'=>$ScAddCount_ST,'SC_UPDATE'=>$ScUpCount_ST,'SC_SELF'=>$ScSelfCount_ST,'OVO'=>$ovoCount_ST,'DYNATRACE'=>$dyCount_ST,'DYNATRACE_SELF'=>$dySelfCount_ST};	
							
						}elsif($rowval->{Assigned_to} eq $UserName->{MG}){
						
							$ScAddCount_MG = $ScAddCount_MG+$rowval->{Reference_4};
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_MG,'BSM_UPDATE'=>$bsmUpCount_MG,'BSM_SELF'=>$bsmSelfCount_MG,'SC_EVP'=>$ScEvpCount_MG,'SC_ADD'=>$ScAddCount_MG,'SC_UPDATE'=>$ScUpCount_MG,'SC_SELF'=>$ScSelfCount_MG,'OVO'=>$ovoCount_MG,'DYNATRACE'=>$dyCount_MG,'DYNATRACE_SELF'=>$dySelfCount_MG};	
							
						}elsif($rowval->{Assigned_to} eq $UserName->{AS}){
						
							$ScAddCount_AS = $ScAddCount_AS+$rowval->{Reference_4};					
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_AS,'BSM_UPDATE'=>$bsmUpCount_AS,'BSM_SELF'=>$bsmSelfCount_AS,'SC_EVP'=>$ScEvpCount_AS,'SC_ADD'=>$ScAddCount_AS,'SC_UPDATE'=>$ScUpCount_AS,'SC_SELF'=>$ScSelfCount_AS,'OVO'=>$ovoCount_AS,'DYNATRACE'=>$dyCount_AS,'DYNATRACE_SELF'=>$dySelfCount_AS};
							
						}elsif($rowval->{Assigned_to} eq $UserName->{RK}){
						
							$ScAddCount_RK = $ScAddCount_RK+$rowval->{Reference_4};
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_RK,'BSM_UPDATE'=>$bsmUpCount_RK,'BSM_SELF'=>$bsmSelfCount_RK,'SC_EVP'=>$ScEvpCount_RK,'SC_ADD'=>$ScAddCount_RK,'SC_UPDATE'=>$ScUpCount_RK,'SC_SELF'=>$ScSelfCount_RK,'OVO'=>$ovoCount_RK,'DYNATRACE'=>$dyCount_RK,'DYNATRACE_SELF'=>$dySelfCount_RK};	
							
						}elsif($rowval->{Assigned_to} eq $UserName->{VSR}){
						
							$ScAddCount_VSR = $ScAddCount_VSR+$rowval->{Reference_4};
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_VSR,'BSM_UPDATE'=>$bsmUpCount_VSR,'BSM_SELF'=>$bsmSelfCount_VSR,'SC_EVP'=>$ScEvpCount_VSR,'SC_ADD'=>$ScAddCount_VSR,'SC_UPDATE'=>$ScUpCount_VSR,'SC_SELF'=>$ScSelfCount_VSR,'OVO'=>$ovoCount_VSR,'DYNATRACE'=>$dyCount_VSR,'DYNATRACE_SELF'=>$dySelfCount_VSR};
							
						}else{
						
							$ScAddCount_VS = $ScAddCount_VS+$rowval->{Reference_4};
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_VS,'BSM_UPDATE'=>$bsmUpCount_VS,'BSM_SELF'=>$bsmSelfCount_VS,'SC_EVP'=>$ScEvpCount_VS,'SC_ADD'=>$ScAddCount_VS,'SC_UPDATE'=>$ScUpCount_VS,'SC_SELF'=>$ScSelfCount_VS,'OVO'=>$ovoCount_VS,'DYNATRACE'=>$dyCount_VS,'DYNATRACE_SELF'=>$dySelfCount_VS};
						}				
						
					}elsif($rowval->{Reference_2} eq 'UPDATE'){
						
						if($rowval->{Assigned_to} eq $UserName->{SM}){

							$ScUpCount_SM = $ScUpCount_SM+$rowval->{Reference_4};
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_SM,'BSM_UPDATE'=>$bsmUpCount_SM,'BSM_SELF'=>$bsmSelfCount_SM,'SC_EVP'=>$ScEvpCount_SM,'SC_ADD'=>$ScAddCount_SM,'SC_UPDATE'=>$ScUpCount_SM,'SC_SELF'=>$ScSelfCount_SM,'OVO'=>$ovoCount_SM,'DYNATRACE'=>$dyCount_SM,'DYNATRACE_SELF'=>$dySelfCount_SM};	

						}elsif($rowval->{Assigned_to} eq $UserName->{ST}){
						
							$ScUpCount_ST = $ScUpCount_ST+$rowval->{Reference_4};
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_ST,'BSM_UPDATE'=>$bsmUpCount_ST,'BSM_SELF'=>$bsmSelfCount_ST,'SC_EVP'=>$ScEvpCount_ST,'SC_ADD'=>$ScAddCount_ST,'SC_UPDATE'=>$ScUpCount_ST,'SC_SELF'=>$ScSelfCount_ST,'OVO'=>$ovoCount_ST,'DYNATRACE'=>$dyCount_ST,'DYNATRACE_SELF'=>$dySelfCount_ST};	
							
						}elsif($rowval->{Assigned_to} eq $UserName->{MG}){
						
							$ScUpCount_MG = $ScUpCount_MG+$rowval->{Reference_4};
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_MG,'BSM_UPDATE'=>$bsmUpCount_MG,'BSM_SELF'=>$bsmSelfCount_MG,'SC_EVP'=>$ScEvpCount_MG,'SC_ADD'=>$ScAddCount_MG,'SC_UPDATE'=>$ScUpCount_MG,'SC_SELF'=>$ScSelfCount_MG,'OVO'=>$ovoCount_MG,'DYNATRACE'=>$dyCount_MG,'DYNATRACE_SELF'=>$dySelfCount_MG};	
							
						}elsif($rowval->{Assigned_to} eq $UserName->{AS}){
						
							$ScUpCount_AS = $ScUpCount_AS+$rowval->{Reference_4};							
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_AS,'BSM_UPDATE'=>$bsmUpCount_AS,'BSM_SELF'=>$bsmSelfCount_AS,'SC_EVP'=>$ScEvpCount_AS,'SC_ADD'=>$ScAddCount_AS,'SC_UPDATE'=>$ScUpCount_AS,'SC_SELF'=>$ScSelfCount_AS,'OVO'=>$ovoCount_AS,'DYNATRACE'=>$dyCount_AS,'DYNATRACE_SELF'=>$dySelfCount_AS};
							
						}elsif($rowval->{Assigned_to} eq $UserName->{RK}){
						
							$ScUpCount_RK = $ScUpCount_RK+$rowval->{Reference_4};
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_RK,'BSM_UPDATE'=>$bsmUpCount_RK,'BSM_SELF'=>$bsmSelfCount_RK,'SC_EVP'=>$ScEvpCount_RK,'SC_ADD'=>$ScAddCount_RK,'SC_UPDATE'=>$ScUpCount_RK,'SC_SELF'=>$ScSelfCount_RK,'OVO'=>$ovoCount_RK,'DYNATRACE'=>$dyCount_RK,'DYNATRACE_SELF'=>$dySelfCount_RK};	
							
						}elsif($rowval->{Assigned_to} eq $UserName->{VSR}){
						
							$ScUpCount_VSR = $ScUpCount_VSR+$rowval->{Reference_4};
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_VSR,'BSM_UPDATE'=>$bsmUpCount_VSR,'BSM_SELF'=>$bsmSelfCount_VSR,'SC_EVP'=>$ScEvpCount_VSR,'SC_ADD'=>$ScAddCount_VSR,'SC_UPDATE'=>$ScUpCount_VSR,'SC_SELF'=>$ScSelfCount_VSR,'OVO'=>$ovoCount_VSR,'DYNATRACE'=>$dyCount_VSR,'DYNATRACE_SELF'=>$dySelfCount_VSR};
							
						}else{
						
							$ScUpCount_VS = $ScUpCount_VS+$rowval->{Reference_4};
							$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_VS,'BSM_UPDATE'=>$bsmUpCount_VS,'BSM_SELF'=>$bsmSelfCount_VS,'SC_EVP'=>$ScEvpCount_VS,'SC_ADD'=>$ScAddCount_VS,'SC_UPDATE'=>$ScUpCount_VS,'SC_SELF'=>$ScSelfCount_VS,'OVO'=>$ovoCount_VS,'DYNATRACE'=>$dyCount_VS,'DYNATRACE_SELF'=>$dySelfCount_VS};
						}
						
					}				
				}
	}elsif($rowval->{Reference_1} eq 'SITESCOPE' && $rowval->{Reference_3} eq 'SELF-SERVICE'){
	
				if($rowval->{Assigned_to} eq $UserName->{SM}){

					$ScSelfCount_SM = $ScSelfCount_SM+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_SM,'BSM_UPDATE'=>$bsmUpCount_SM,'BSM_SELF'=>$bsmSelfCount_SM,'SC_EVP'=>$ScEvpCount_SM,'SC_ADD'=>$ScAddCount_SM,'SC_UPDATE'=>$ScUpCount_SM,'SC_SELF'=>$ScSelfCount_SM,'OVO'=>$ovoCount_SM,'DYNATRACE'=>$dyCount_SM,'DYNATRACE_SELF'=>$dySelfCount_SM};	

				}elsif($rowval->{Assigned_to} eq $UserName->{ST}){
				
					$ScSelfCount_ST = $ScSelfCount_ST+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_ST,'BSM_UPDATE'=>$bsmUpCount_ST,'BSM_SELF'=>$bsmSelfCount_ST,'SC_EVP'=>$ScEvpCount_ST,'SC_ADD'=>$ScAddCount_ST,'SC_UPDATE'=>$ScUpCount_ST,'SC_SELF'=>$ScSelfCount_ST,'OVO'=>$ovoCount_ST,'DYNATRACE'=>$dyCount_ST,'DYNATRACE_SELF'=>$dySelfCount_ST};
					
				}					
				elsif($rowval->{Assigned_to} eq $UserName->{MG}){
				
					$ScSelfCount_MG = $ScSelfCount_MG+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_MG,'BSM_UPDATE'=>$bsmUpCount_MG,'BSM_SELF'=>$bsmSelfCount_MG,'SC_EVP'=>$ScEvpCount_MG,'SC_ADD'=>$ScAddCount_MG,'SC_UPDATE'=>$ScUpCount_MG,'SC_SELF'=>$ScSelfCount_MG,'OVO'=>$ovoCount_MG,'DYNATRACE'=>$dyCount_MG,'DYNATRACE_SELF'=>$dySelfCount_MG};	
					
				}elsif($rowval->{Assigned_to} eq $UserName->{AS}){
				
					$ScSelfCount_AS = $ScSelfCount_AS+$rowval->{Reference_4};					
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_AS,'BSM_UPDATE'=>$bsmUpCount_AS,'BSM_SELF'=>$bsmSelfCount_AS,'SC_EVP'=>$ScEvpCount_AS,'SC_ADD'=>$ScAddCount_AS,'SC_UPDATE'=>$ScUpCount_AS,'SC_SELF'=>$ScSelfCount_AS,'OVO'=>$ovoCount_AS,'DYNATRACE'=>$dyCount_AS,'DYNATRACE_SELF'=>$dySelfCount_AS};
					
				}elsif($rowval->{Assigned_to} eq $UserName->{RK}){
				
					$ScSelfCount_RK = $ScSelfCount_RK+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_RK,'BSM_UPDATE'=>$bsmUpCount_RK,'BSM_SELF'=>$bsmSelfCount_RK,'SC_EVP'=>$ScEvpCount_RK,'SC_ADD'=>$ScAddCount_RK,'SC_UPDATE'=>$ScUpCount_RK,'SC_SELF'=>$ScSelfCount_RK,'OVO'=>$ovoCount_RK,'DYNATRACE'=>$dyCount_RK,'DYNATRACE_SELF'=>$dySelfCount_RK};	
					
				}elsif($rowval->{Assigned_to} eq $UserName->{VSR}){
				
					$ScSelfCount_VSR = $ScSelfCount_VSR+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_VSR,'BSM_UPDATE'=>$bsmUpCount_VSR,'BSM_SELF'=>$bsmSelfCount_VSR,'SC_EVP'=>$ScEvpCount_VSR,'SC_ADD'=>$ScAddCount_VSR,'SC_UPDATE'=>$ScUpCount_VSR,'SC_SELF'=>$ScSelfCount_VSR,'OVO'=>$ovoCount_VSR,'DYNATRACE'=>$dyCount_VSR,'DYNATRACE_SELF'=>$dySelfCount_VSR};
					
				}else{
				
					$ScSelfCount_VS = $ScSelfCount_VS+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_VS,'BSM_UPDATE'=>$bsmUpCount_VS,'BSM_SELF'=>$bsmSelfCount_VS,'SC_EVP'=>$ScEvpCount_VS,'SC_ADD'=>$ScAddCount_VS,'SC_UPDATE'=>$ScUpCount_VS,'SC_SELF'=>$ScSelfCount_VS,'OVO'=>$ovoCount_VS,'DYNATRACE'=>$dyCount_VS,'DYNATRACE_SELF'=>$dySelfCount_VS};
				}
	}
	elsif($rowval->{Reference_1} eq 'OVO'){				
			
			if($rowval->{Reference_2} =~m/ADD|UPDATE|DELETE|DISABLE|ENABLE/){
	
			if($rowval->{Assigned_to} eq $UserName->{SM}){

					$ovoCount_SM = $ovoCount_SM+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_SM,'BSM_UPDATE'=>$bsmUpCount_SM,'BSM_SELF'=>$bsmSelfCount_SM,'SC_EVP'=>$ScEvpCount_SM,'SC_ADD'=>$ScAddCount_SM,'SC_UPDATE'=>$ScUpCount_SM,'SC_SELF'=>$ScSelfCount_SM,'OVO'=>$ovoCount_SM,'DYNATRACE'=>$dyCount_SM,'DYNATRACE_SELF'=>$dySelfCount_SM};	

			}elsif($rowval->{Assigned_to} eq $UserName->{ST}){
				
					$ovoCount_ST = $ovoCount_ST+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_ST,'BSM_UPDATE'=>$bsmUpCount_ST,'BSM_SELF'=>$bsmSelfCount_ST,'SC_EVP'=>$ScEvpCount_ST,'SC_ADD'=>$ScAddCount_ST,'SC_UPDATE'=>$ScUpCount_ST,'SC_SELF'=>$ScSelfCount_ST,'OVO'=>$ovoCount_ST,'DYNATRACE'=>$dyCount_ST,'DYNATRACE_SELF'=>$dySelfCount_ST};
					
			}elsif($rowval->{Assigned_to} eq $UserName->{MG}){
				
					$ovoCount_MG = $ovoCount_MG+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_MG,'BSM_UPDATE'=>$bsmUpCount_MG,'BSM_SELF'=>$bsmSelfCount_MG,'SC_EVP'=>$ScEvpCount_MG,'SC_ADD'=>$ScAddCount_MG,'SC_UPDATE'=>$ScUpCount_MG,'SC_SELF'=>$ScSelfCount_MG,'OVO'=>$ovoCount_MG,'DYNATRACE'=>$dyCount_MG,'DYNATRACE_SELF'=>$dySelfCount_MG};	
					
			}elsif($rowval->{Assigned_to} eq $UserName->{AS}){
				
					$ovoCount_AS = $ovoCount_AS+$rowval->{Reference_4};
					#print("akshat---ovo section----$bsmAddCount_AS\n");
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_AS,'BSM_UPDATE'=>$bsmUpCount_AS,'BSM_SELF'=>$bsmSelfCount_AS,'SC_EVP'=>$ScEvpCount_AS,'SC_ADD'=>$ScAddCount_AS,'SC_UPDATE'=>$ScUpCount_AS,'SC_SELF'=>$ScSelfCount_AS,'OVO'=>$ovoCount_AS,'DYNATRACE'=>$dyCount_AS,'DYNATRACE_SELF'=>$dySelfCount_AS};
					
			}elsif($rowval->{Assigned_to} eq $UserName->{RK}){
				
					$ovoCount_RK = $ovoCount_RK+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_RK,'BSM_UPDATE'=>$bsmUpCount_RK,'BSM_SELF'=>$bsmSelfCount_RK,'SC_EVP'=>$ScEvpCount_RK,'SC_ADD'=>$ScAddCount_RK,'SC_UPDATE'=>$ScUpCount_RK,'SC_SELF'=>$ScSelfCount_RK,'OVO'=>$ovoCount_RK,'DYNATRACE'=>$dyCount_RK,'DYNATRACE_SELF'=>$dySelfCount_RK};	
					
			}elsif($rowval->{Assigned_to} eq $UserName->{VSR}){
				
					$ovoCount_VSR = $ovoCount_VSR+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_VSR,'BSM_UPDATE'=>$bsmUpCount_VSR,'BSM_SELF'=>$bsmSelfCount_VSR,'SC_EVP'=>$ScEvpCount_VSR,'SC_ADD'=>$ScAddCount_VSR,'SC_UPDATE'=>$ScUpCount_VSR,'SC_SELF'=>$ScSelfCount_VSR,'OVO'=>$ovoCount_VSR,'DYNATRACE'=>$dyCount_VSR,'DYNATRACE_SELF'=>$dySelfCount_VSR};
					
			}else{
				
					$ovoCount_VS = $ovoCount_VS+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_VS,'BSM_UPDATE'=>$bsmUpCount_VS,'BSM_SELF'=>$bsmSelfCount_VS,'SC_EVP'=>$ScEvpCount_VS,'SC_ADD'=>$ScAddCount_VS,'SC_UPDATE'=>$ScUpCount_VS,'SC_SELF'=>$ScSelfCount_VS,'OVO'=>$ovoCount_VS,'DYNATRACE'=>$dyCount_VS,'DYNATRACE_SELF'=>$dySelfCount_VS};
			}

		}
	
	}
	elsif($rowval->{Reference_1} eq 'DYNATRACE' && $rowval->{Reference_3} ne 'SELF-SERVICE'){
	
			if($rowval->{Reference_2} =~m/ADD|UPDATE/){
	
			if($rowval->{Assigned_to} eq $UserName->{SM}){

					$dyCount_SM = $dyCount_SM+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_SM,'BSM_UPDATE'=>$bsmUpCount_SM,'BSM_SELF'=>$bsmSelfCount_SM,'SC_EVP'=>$ScEvpCount_SM,'SC_ADD'=>$ScAddCount_SM,'SC_UPDATE'=>$ScUpCount_SM,'SC_SELF'=>$ScSelfCount_SM,'OVO'=>$ovoCount_SM,'DYNATRACE'=>$dyCount_SM,'DYNATRACE_SELF'=>$dySelfCount_SM};	

			}elsif($rowval->{Assigned_to} eq $UserName->{ST}){
				
					$dyCount_ST = $dyCount_ST+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_ST,'BSM_UPDATE'=>$bsmUpCount_ST,'BSM_SELF'=>$bsmSelfCount_ST,'SC_EVP'=>$ScEvpCount_ST,'SC_ADD'=>$ScAddCount_ST,'SC_UPDATE'=>$ScUpCount_ST,'SC_SELF'=>$ScSelfCount_ST,'OVO'=>$ovoCount_ST,'DYNATRACE'=>$dyCount_ST,'DYNATRACE_SELF'=>$dySelfCount_ST};
					
			}elsif($rowval->{Assigned_to} eq $UserName->{MG}){
				
					$dyCount_MG = $dyCount_MG+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_MG,'BSM_UPDATE'=>$bsmUpCount_MG,'BSM_SELF'=>$bsmSelfCount_MG,'SC_EVP'=>$ScEvpCount_MG,'SC_ADD'=>$ScAddCount_MG,'SC_UPDATE'=>$ScUpCount_MG,'SC_SELF'=>$ScSelfCount_MG,'OVO'=>$ovoCount_MG,'DYNATRACE'=>$dyCount_MG,'DYNATRACE_SELF'=>$dySelfCount_MG};	
					
			}elsif($rowval->{Assigned_to} eq $UserName->{AS}){
				
					$dyCount_AS = $dyCount_AS+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_AS,'BSM_UPDATE'=>$bsmUpCount_AS,'BSM_SELF'=>$bsmSelfCount_AS,'SC_EVP'=>$ScEvpCount_AS,'SC_ADD'=>$ScAddCount_AS,'SC_UPDATE'=>$ScUpCount_AS,'SC_SELF'=>$ScSelfCount_AS,'OVO'=>$ovoCount_AS,'DYNATRACE'=>$dyCount_AS,'DYNATRACE_SELF'=>$dySelfCount_AS};
					
			}elsif($rowval->{Assigned_to} eq $UserName->{RK}){
				
					$dyCount_RK = $dyCount_RK+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_RK,'BSM_UPDATE'=>$bsmUpCount_RK,'BSM_SELF'=>$bsmSelfCount_RK,'SC_EVP'=>$ScEvpCount_RK,'SC_ADD'=>$ScAddCount_RK,'SC_UPDATE'=>$ScUpCount_RK,'SC_SELF'=>$ScSelfCount_RK,'OVO'=>$ovoCount_RK,'DYNATRACE'=>$dyCount_RK,'DYNATRACE_SELF'=>$dySelfCount_RK};	
					
			}elsif($rowval->{Assigned_to} eq $UserName->{VSR}){
				
					$dyCount_VSR = $dyCount_VSR+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_VSR,'BSM_UPDATE'=>$bsmUpCount_VSR,'BSM_SELF'=>$bsmSelfCount_VSR,'SC_EVP'=>$ScEvpCount_VSR,'SC_ADD'=>$ScAddCount_VSR,'SC_UPDATE'=>$ScUpCount_VSR,'SC_SELF'=>$ScSelfCount_VSR,'OVO'=>$ovoCount_VSR,'DYNATRACE'=>$dyCount_VSR,'DYNATRACE_SELF'=>$dySelfCount_VSR};
					
			}else{
				
					$dyCount_VS = $dyCount_VS+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_VS,'BSM_UPDATE'=>$bsmUpCount_VS,'BSM_SELF'=>$bsmSelfCount_VS,'SC_EVP'=>$ScEvpCount_VS,'SC_ADD'=>$ScAddCount_VS,'SC_UPDATE'=>$ScUpCount_VS,'SC_SELF'=>$ScSelfCount_VS,'OVO'=>$ovoCount_VS,'DYNATRACE'=>$dyCount_VS,'DYNATRACE_SELF'=>$dySelfCount_VS};
			}
		}
	#dynatrace
	
	}elsif($rowval->{Reference_1} eq 'DYNATRACE' && $rowval->{Reference_3} eq 'SELF-SERVICE'){
	
			if($rowval->{Reference_2} =~m/ADD|UPDATE/){
	
			if($rowval->{Assigned_to} eq $UserName->{SM}){

					$dySelfCount_SM = $dySelfCount_SM+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_SM,'BSM_UPDATE'=>$bsmUpCount_SM,'BSM_SELF'=>$bsmSelfCount_SM,'SC_EVP'=>$ScEvpCount_SM,'SC_ADD'=>$ScAddCount_SM,'SC_UPDATE'=>$ScUpCount_SM,'SC_SELF'=>$ScSelfCount_SM,'OVO'=>$ovoCount_SM,'DYNATRACE'=>$dyCount_SM,'DYNATRACE_SELF'=>$dySelfCount_SM};	

			}elsif($rowval->{Assigned_to} eq $UserName->{ST}){
				
					$dySelfCount_ST = $dySelfCount_ST+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_ST,'BSM_UPDATE'=>$bsmUpCount_ST,'BSM_SELF'=>$bsmSelfCount_ST,'SC_EVP'=>$ScEvpCount_ST,'SC_ADD'=>$ScAddCount_ST,'SC_UPDATE'=>$ScUpCount_ST,'SC_SELF'=>$ScSelfCount_ST,'OVO'=>$ovoCount_ST,'DYNATRACE'=>$dyCount_ST,'DYNATRACE_SELF'=>$dySelfCount_ST};
					
			}elsif($rowval->{Assigned_to} eq $UserName->{MG}){
				
					$dySelfCount_MG = $dySelfCount_MG+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_MG,'BSM_UPDATE'=>$bsmUpCount_MG,'BSM_SELF'=>$bsmSelfCount_MG,'SC_EVP'=>$ScEvpCount_MG,'SC_ADD'=>$ScAddCount_MG,'SC_UPDATE'=>$ScUpCount_MG,'SC_SELF'=>$ScSelfCount_MG,'OVO'=>$ovoCount_MG,'DYNATRACE'=>$dyCount_MG,'DYNATRACE_SELF'=>$dySelfCount_MG};	
					
			}elsif($rowval->{Assigned_to} eq $UserName->{AS}){		
			
					$dySelfCount_AS = $dySelfCount_AS+$rowval->{Reference_4};					
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_AS,'BSM_UPDATE'=>$bsmUpCount_AS,'BSM_SELF'=>$bsmSelfCount_AS,'SC_EVP'=>$ScEvpCount_AS,'SC_ADD'=>$ScAddCount_AS,'SC_UPDATE'=>$ScUpCount_AS,'SC_SELF'=>$ScSelfCount_AS,'OVO'=>$ovoCount_AS,'DYNATRACE'=>$dyCount_AS,'DYNATRACE_SELF'=>$dySelfCount_AS};
					
			}elsif($rowval->{Assigned_to} eq $UserName->{RK}){
				
					$dySelfCount_RK = $dySelfCount_RK+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_RK,'BSM_UPDATE'=>$bsmUpCount_RK,'BSM_SELF'=>$bsmSelfCount_RK,'SC_EVP'=>$ScEvpCount_RK,'SC_ADD'=>$ScAddCount_RK,'SC_UPDATE'=>$ScUpCount_RK,'SC_SELF'=>$ScSelfCount_RK,'OVO'=>$ovoCount_RK,'DYNATRACE'=>$dyCount_RK,'DYNATRACE_SELF'=>$dySelfCount_RK};	
					
			}elsif($rowval->{Assigned_to} eq $UserName->{VSR}){
				
					$dySelfCount_VSR = $dySelfCount_VSR+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_VSR,'BSM_UPDATE'=>$bsmUpCount_VSR,'BSM_SELF'=>$bsmSelfCount_VSR,'SC_EVP'=>$ScEvpCount_VSR,'SC_ADD'=>$ScAddCount_VSR,'SC_UPDATE'=>$ScUpCount_VSR,'SC_SELF'=>$ScSelfCount_VSR,'OVO'=>$ovoCount_VSR,'DYNATRACE'=>$dyCount_VSR,'DYNATRACE_SELF'=>$dySelfCount_VSR};
					
			}else{
				
					$dySelfCount_VS = $dySelfCount_VS+$rowval->{Reference_4};
					$hashname->{$rowval->{Assigned_to}} = {'BSM_ADD'=>$bsmAddCount_VS,'BSM_UPDATE'=>$bsmUpCount_VS,'BSM_SELF'=>$bsmSelfCount_VS,'SC_EVP'=>$ScEvpCount_VS,'SC_ADD'=>$ScAddCount_VS,'SC_UPDATE'=>$ScUpCount_VS,'SC_SELF'=>$ScSelfCount_VS,'OVO'=>$ovoCount_VS,'DYNATRACE'=>$dyCount_VS,'DYNATRACE_SELF'=>$dySelfCount_VS};
			}	
	}
	
	}else{
		next;
	}
}

print ("month value---$month_hash->{$month_Value}");

my $datestart 	=	"$month_hash->{$month_Value}_$yearval";
my $month_name		=	"$month_hash->{$month_Value} $yearval";
print("month value -----------$month_Value\n");
print("year value -----------$yearval\n");

print ("date start val -----$datestart\n");

#print Dumper($hashname);

####################################### User wise Probe Deployment Count ######################################

my $row_val  = 1;
my $cell_no  = 2;

my $workbook  = Excel::Writer::XLSX->new("C:\\Users\\vsrivas3\\Desktop\\Report\\Monthly_Report_data_$datestart\.xlsx");
my $worksheet = $workbook->add_worksheet("UserWiseProbeCount");
   $worksheet->set_column('A:A', 18);
   $worksheet->set_column('B:B', 10);
my $format = $workbook->add_format(border=>1);
my $format1 = $workbook->add_format(border=>1);
my $format2 = $workbook->add_format(border=>1);
my $format3 = $workbook->add_format(border=>1);
$format1->set_bg_color( 'orange' );
$format1->set_text_wrap();
$format1->set_align( 'vcenter' );
$format1->set_size(9);
$format->set_size(9);
$format->set_align( 'center' );
$format2->set_size(10);
$format2->set_align( 'center' );
$format2->set_bg_color( 'orange' );
$format2->set_bold();
$format3->set_size(9);
$format3->set_align( 'Left' );

$worksheet->write("A$row_val", "Team Member",$format1);
$worksheet->write("B$row_val", "New BSM Probes",$format1);
$worksheet->write("C$row_val", "BSM probe -Break Fixes",$format1);
$worksheet->write("D$row_val", "Sitescope Monitors Updated /Break-Fix",$format1);
$worksheet->write("E$row_val", "Sitescope Monitors newly added",$format1);
$worksheet->write("F$row_val", "OVO templates added/updated/Deleted (specific to number of servers)",$format1);
$worksheet->write("G$row_val", "Dynatrace Deployments",$format1);
$worksheet->write("H$row_val", "EVP Sitescope deployment",$format1);
$worksheet->write("I$row_val", "Self Service BSM Probe",$format1);
$worksheet->write("J$row_val", "Self Service Dynatrace Probes",$format1);
$worksheet->write("K$row_val", "Self Service Sitescope Probes",$format1);
#$worksheet->write("L$row_val", "DeploymenTotal",$format1);

my($bsmTotalAdd,$bsmTotalUp,$ScTotalUp,$ScTotalAdd,$OvoTotal,$DyTotal,$EvpTotal,$bsmTotalSelf,$DyTotalself,$ScTotalSelf,$deptotal) = (0,0,0,0,0,0,0,0,0,0,0);

#print ("Team Member|New BSM Probes|BSM probe -Break Fixes|Sitescope Monitors Updated /Break-Fix|Sitescope Monitors newly added|OVO templates added/updated/Deleted (specific to number of servers)|Dynatrace Deployments|EVP Sitescope deployment|Self Service BSM Probe|Self Service Dynatrace Probes|Self Service Sitescope Probes|DeploymenTotal\n");

foreach my $teamMember (keys %$hashname){

	$bsmTotalAdd	=	$bsmTotalAdd+$hashname->{$teamMember}->{BSM_ADD};
	$bsmTotalUp		=	$bsmTotalUp+$hashname->{$teamMember}->{BSM_UPDATE};
	$ScTotalUp		=	$ScTotalUp+$hashname->{$teamMember}->{SC_UPDATE};
	$ScTotalAdd		=	$ScTotalAdd+$hashname->{$teamMember}->{SC_ADD};
	$OvoTotal		=	$OvoTotal+$hashname->{$teamMember}->{OVO};
	$DyTotal		=	$DyTotal+$hashname->{$teamMember}->{DYNATRACE};
	$EvpTotal		=	$EvpTotal+$hashname->{$teamMember}->{SC_EVP};
	$bsmTotalSelf	=	$bsmTotalSelf+$hashname->{$teamMember}->{BSM_SELF};
	$DyTotalself	=	$DyTotalself+$hashname->{$teamMember}->{DYNATRACE_SELF};
	$ScTotalSelf	=	$ScTotalSelf+$hashname->{$teamMember}->{SC_SELF};

	$worksheet->write("A".$cell_no, $teamMember,$format3);
	$worksheet->write("B".$cell_no, $hashname->{$teamMember}->{BSM_ADD},$format);
	$worksheet->write("C".$cell_no, $hashname->{$teamMember}->{BSM_UPDATE},$format);
	$worksheet->write("D".$cell_no, $hashname->{$teamMember}->{SC_UPDATE},$format);
	$worksheet->write("E".$cell_no, $hashname->{$teamMember}->{SC_ADD},$format);
	$worksheet->write("F".$cell_no, $hashname->{$teamMember}->{OVO},$format);
	$worksheet->write("G".$cell_no, $hashname->{$teamMember}->{DYNATRACE},$format);
	$worksheet->write("H".$cell_no, $hashname->{$teamMember}->{SC_EVP},$format);
	$worksheet->write("I".$cell_no, $hashname->{$teamMember}->{BSM_SELF},$format);
	$worksheet->write("J".$cell_no, $hashname->{$teamMember}->{DYNATRACE_SELF},$format);
	$worksheet->write("K".$cell_no, $hashname->{$teamMember}->{SC_SELF},$format);
	
	#print FILEOUT ("$teamMember|$hashname->{$teamMember}->{BSM_ADD}|$hashname->{$teamMember}->{BSM_UPDATE}|$hashname->{$teamMember}->{SC_UPDATE}|$hashname->{$teamMember}->{SC_ADD}|$hashname->{$teamMember}->{OVO}|$hashname->{$teamMember}->{DYNATRACE}|$hashname->{$teamMember}->{SC_EVP}|$hashname->{$teamMember}->{BSM_SELF}|$hashname->{$teamMember}->{DYNATRACE_SELF}|$hashname->{$teamMember}->{SC_SELF}\n");
	$cell_no++;
}
	
	$deptotal = $bsmTotalAdd+$bsmTotalUp+$ScTotalUp+$ScTotalAdd+$OvoTotal+$DyTotal+$EvpTotal+$bsmTotalSelf+$DyTotalself+$ScTotalSelf;
	
	$worksheet->write("A".$cell_no, "Total",$format2);
	$worksheet->write("B".$cell_no, $bsmTotalAdd,$format2);
	$worksheet->write("C".$cell_no, $bsmTotalUp,$format2);
	$worksheet->write("D".$cell_no, $ScTotalUp,$format2);
	$worksheet->write("E".$cell_no, $ScTotalAdd,$format2);
	$worksheet->write("F".$cell_no, $OvoTotal,$format2);
	$worksheet->write("G".$cell_no, $DyTotal,$format2);
	$worksheet->write("H".$cell_no, $EvpTotal,$format2);
	$worksheet->write("I".$cell_no, $bsmTotalSelf,$format2);
	$worksheet->write("J".$cell_no, $DyTotalself,$format2);
	$worksheet->write("K".$cell_no, $ScTotalSelf,$format2);
	
	#############################################################################
	
	##########################SR CLosed by per Person ###########################
	
	my $cn_person 	=	$cell_no+4;
	my $sr_total 	=	0;
	$worksheet->write("A$cn_person", "Team Member",$format2);
	$worksheet->write("B$cn_person", "Req Closed",$format2);
	
	foreach my $User_name (keys %$userhash){
		$cn_person++;
		$sr_total	=	$sr_total+$userhash->{$User_name};
		$worksheet->write("A$cn_person", $User_name,$format3);
		$worksheet->write("B$cn_person", $userhash->{$User_name},$format);		
	}
		$cn_person	=	$cn_person+1;
		$worksheet->write("A$cn_person", "Total",$format3);
		$worksheet->write("B$cn_person", $sr_total,$format);
	
	#############################################################################
	
	######################### Probe Deployment Count ############################
	
	my $worksheet1 = $workbook->add_worksheet("DeploymentCount");
	my $cell_Pc		=	0;	
	$worksheet1->set_column('A:A', 36);
	
	
	my $cell_type 	= $cell_Pc+1;
	my $cell_req	= $cell_type+1;
	my $cell_dep	= $cell_req+1;
	my $nor_req		= $cell_dep+1;
	my $cell_self	= $nor_req+1;
	my $cell_uplift	= $cell_self+1;
	my $cell_Esc	= $cell_uplift+1;
	my $cell_Cba	= $cell_Esc+1;
	my $cell_Av		= $cell_Cba+1;	
	
	my $avgTime 	= $datediff/$totalTicketCount;
	my $normal_reqCount	=	$totalTicketCount-($selfTicketCount+$moniUpliftTicketCount+$escalatedTicketCount);

	$worksheet1->write("A".$cell_type, "COUNT TYPE",$format1);
	$worksheet1->write("B".$cell_type, "COUNT NO",$format1);
	$worksheet1->write("A".$cell_req, "Total Req Closed",$format3);
	$worksheet1->write("B".$cell_req, $totalTicketCount,$format);
	$worksheet1->write("A".$cell_dep, "Total Deployment",$format3);
	$worksheet1->write("B".$cell_dep, $deptotal,$format);
	$worksheet1->write("A".$nor_req, "Normal Req Closed",$format3);
	$worksheet1->write("B".$nor_req, $normal_reqCount,$format);	
	$worksheet1->write("A".$cell_self, "Self Service Req. Completed",$format3);
	$worksheet1->write("B".$cell_self, $selfTicketCount,$format);
	$worksheet1->write("A".$cell_uplift, "Monitoring Uplift Req Completed",$format3);
	$worksheet1->write("B".$cell_uplift, $moniUpliftTicketCount,$format);
	$worksheet1->write("A".$cell_Esc, "Escalated Req Completed",$format3);
	$worksheet1->write("B".$cell_Esc, $escalatedTicketCount,$format);
	$worksheet1->write("A".$cell_Cba, "CBA Req Completed",$format3);
	$worksheet1->write("B".$cell_Cba, $cbaTicketCount,$format);
	$worksheet1->write("A".$cell_Av, "Avarage Time To Complete a Request (In days)",$format3);
	$worksheet1->write("B".$cell_Av, $avgTime,$format);
	
################################################################################################

############################## Month Wise Probe Deployment Count ###############################
	
	my $cn_month 	=	$cn_person+4;	
	$worksheet->write("A$cn_month", "Month",$format1);	
	$worksheet->write("B$cn_month", "New BSM Probes",$format1);
	$worksheet->write("C$cn_month", "BSM probe -Break Fixes",$format1);
	$worksheet->write("D$cn_month", "Sitescope Monitors Updated /Break-Fix",$format1);
	$worksheet->write("E$cn_month", "Sitescope Monitors newly added",$format1);
	$worksheet->write("F$cn_month", "OVO templates added/updated/Deleted (specific to number of servers)",$format1);
	$worksheet->write("G$cn_month", "Dynatrace Deployments",$format1);
	$worksheet->write("H$cn_month", "EVP Sitescope deployment",$format1);
	$worksheet->write("I$cn_month", "Self Service BSM Probe",$format1);
	$worksheet->write("J$cn_month", "Self Service Dynatrace Probes",$format1);
	$worksheet->write("K$cn_month", "Self Service Sitescope Probes",$format1);
	$worksheet->write("L$cn_month", "Total",$format1);
	$cn_month++;	
	$worksheet->write("A$cn_month", $month_name,$format3);
	$worksheet->write("B$cn_month", $bsmTotalAdd,$format);
	$worksheet->write("C$cn_month", $bsmTotalUp,$format);
	$worksheet->write("D$cn_month", $ScTotalUp,$format);
	$worksheet->write("E$cn_month", $ScTotalAdd,$format);
	$worksheet->write("F$cn_month", $OvoTotal,$format);
	$worksheet->write("G$cn_month", $DyTotal,$format);
	$worksheet->write("H$cn_month", $EvpTotal,$format);
	$worksheet->write("I$cn_month", $bsmTotalSelf,$format);
	$worksheet->write("J$cn_month", $DyTotalself,$format);
	$worksheet->write("K$cn_month", $ScTotalSelf,$format);
	$worksheet->write("L$cn_month", $deptotal,$format);
	
	
	my $cn_Reqmonth 	=	$cn_month+4;
	$worksheet->write("A$cn_Reqmonth", "Month",$format1);	
	$worksheet->write("B$cn_Reqmonth", "Escalated high priority requests (all E1, E2 and E3)",$format1);
	$worksheet->write("C$cn_Reqmonth", "Normal Requests",$format1);
	$worksheet->write("D$cn_Reqmonth", "Self Service requests (S)",$format1);
	$worksheet->write("E$cn_Reqmonth", "Monitoring Upliftment (AM)",$format1);
	$worksheet->write("F$cn_Reqmonth", "Total",$format1);
	$cn_Reqmonth++;
	$worksheet->write("A$cn_Reqmonth", $month_name,$format3);	
	$worksheet->write("B$cn_Reqmonth", $escalatedTicketCount,$format);
	$worksheet->write("C$cn_Reqmonth", $normal_reqCount,$format);
	$worksheet->write("D$cn_Reqmonth", $selfTicketCount,$format);
	$worksheet->write("E$cn_Reqmonth", $moniUpliftTicketCount,$format);
	$worksheet->write("F$cn_Reqmonth", $totalTicketCount,$format);
	
#print FILEOUT ("Total|$bsmTotalAdd|$bsmTotalUp|$ScTotalUp|$ScTotalAdd|$OvoTotal|$DyTotal|$EvpTotal|$bsmTotalSelf|$DyTotalself|$ScTotalSelf|$deptotal\n");
#close FILEOUT;

sub dateDifferenceindays{
	my($st_Date,$en_Date) = @_;
	my ($month,$year);

	my @date_Start = split(/\W/,$st_Date);	
	my @date_End = split(/\W/,$en_Date);
	my @first = ($date_Start[2], $date_Start[0], $date_Start[1]);
	my @second = ($date_End[2], $date_End[0], $date_End[1]);
	my $dd = Delta_Days( @first, @second );	
	$month		=	$date_End[0];
	$year		=	$date_End[2];
	return ($dd,$month,$year);
 }
 
close FILEIN;

