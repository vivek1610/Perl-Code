#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;


my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
my $now = sprintf("%02d_%02d_%04d_%02d_%02d_%02d", $mday, $mon+1, $year+1900, $hour, $min, $sec);
my $file_path  = '/home/xavient/analysis/';

my @filearray  = `cd $file_path; ls -Art act_20*.txt`;
chomp @filearray;
my $newFile  =  $file_path."act_new.txt";
open(NEWFILE,">$newFile")|| die "Could not Create file: $!";
foreach my $fvar (@filearray){
my $newhash;
open(ARGV,$file_path.$fvar)|| die "Could not open $fvar: $!";
my @col;
while (<ARGV>){
 $newhash = {
                'ssv_customerid' => '-',
                'ssv_purchdate'  => '-',
                'ssv_modelnumb'  => '-',
                'ssv_purchstore' => '-',
                'ssv_itemsku'    => '-',
                'ssv_tranamount' => '-',
                'ssv_transid'    => '-',
                'ssv_userid'     => '-',
                'ssv_regprod'    => '-',
		'ssv_orderid'    => '-',
		'ssv_discdate'   => '-',
		'ssv_disctitle'  => '-',
		'ssv_disctype'   => '-',
		'ssv_xp1random'  => '-',
		'ssv_pro_ca_ii'  => '-',
		'ssv_movietitle' => '-',
		'ssv_ticketdate' => '-',
		'ssv_ticketqty'  => '-',
		'ssv_pixels_aid' => '-',
		'ssv_pixels_cid' => '-',
		'ssv_dcmpid'     => '-',
		'ssv_userid01'   => '-'
	};
chomp $_;
@col = split ('\t',$_);
#print "q string val-----------$col[12]\n";
if($col[12] ne ''){
my @qsring      = split('\&',$col[12]);
#print "1-  $qsring[0] 2 - $qsring[1] 3 - $qsring[2] 3 - $qsring[3] 4 -  $qsring[4] 5 - $qsring[0] 6 - $qsring[5] 7 - $qsring[6] 8 - $qsring[7] 8 - $qsring[9]\n";
my $hashvar;
foreach my $var  (@qsring){
        my ($key,$val) = split('\=', $var);
	if($key =~m/ssv_customerid|ssv_purchdate|ssv_modelnumb|ssv_purchstore|ssv_itemsku|ssv_tranamount|ssv_transid|ssv_userid|ssv_regprod|ssv_orderid|ssv_discdate|ssv_disctitle|ssv_disctype|ssv_xp1random|ssv_pro_ca_ii|ssv_movietitle|ssv_ticketdate|ssv_ticketqty|ssv_pixels_aid|ssv_pixels_cid|ssv_dcmpid|ssv_userid01/){
        	$hashvar->{$key}=$val;
	}
	else{
	open(ERRORFILE,'>errorLog.txt')|| die "Could not Create Log file: $!";
	print  "ERROR File Name :: $fvar\n";
        print  "Error Line in $fvar :: $_\n";
        print  "Query String Contain this key ::$key and Value :: $val out side of define list\n";
	print ERRORFILE  "ERROR File Name :: $fvar\n\n";
	print ERRORFILE  "Error Line in $fvar :: $_\n\n";
	print ERRORFILE  "Query String Contain this key ::$key and Value :: $val out side of define list\n\n";
	exit;
	
	}
}
foreach my $var1 (keys %{$newhash}){
           if($hashvar->{$var1}){
                $newhash->{$var1} = $hashvar->{$var1};
           }else{
                $newhash->{$var1} = $newhash->{$var1};

           }
}

}

print NEWFILE  "$col[0]\t$col[1]\t$col[2]\t$col[3]\t$col[4]\t$col[5]\t$col[6]\t$col[7]\t$col[8]\t$col[9]\t$col[10]\t$col[11]\t$col[12]\t$col[13]\t$col[14]\t$col[15]\t$col[16]\t$col[17]\t$col[18]\t$col[19]\t$col[20]\t$col[21]\t$col[22]\t$col[23]\t$col[24]\t$col[25]\t$col[26]\t$col[27]\t$col[28]\t$col[29]\t$col[30]\t$col[31]\t$col[32]\t$col[33]\t$col[34]\t$col[35]\t$col[36]\t$col[37]\t$col[38]\t$col[39]\t$col[40]\t$col[41]\t$col[42]\t$col[43]\t$col[44]\t$col[45]\t$col[46]\t$col[47]\t$col[48]\t$col[49]\t$col[50]\t$col[51]\t$col[52]\t$newhash->{ssv_customerid}\t$newhash->{ssv_purchdate}\t$newhash->{ssv_modelnumb}\t$newhash->{ssv_purchstore}\t$newhash->{ssv_itemsku}\t$newhash->{ssv_tranamount}\t$newhash->{ssv_transid}\t$newhash->{ssv_userid}\t$newhash->{ssv_regprod}\t$newhash->{ssv_orderid}\t$newhash->{ssv_discdate}\t$newhash->{ssv_disctitle}\t$newhash->{ssv_disctype}\t$newhash->{ssv_xp1random}\t$newhash->{ssv_pro_ca_ii}\t$newhash->{ssv_movietitle}\t$newhash->{ssv_ticketdate}\t$newhash->{ssv_ticketqty}\t$newhash->{ssv_pixels_aid}\t$newhash->{ssv_pixels_cid}\t$newhash->{ssv_dcmpid}\t$newhash->{ssv_userid01}\n";
}
}
close NEWFILE;

