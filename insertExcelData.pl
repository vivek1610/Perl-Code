#!/usr/bin/perl
use strict;
use warnings;
use DBI;

my $driver = "mysql";
my $database = "Alert";
my $dsn = "DBI:$driver:database=$database";
my $userid = "root";
my $password = "root";
my $dbh = DBI->connect($dsn, $userid, $password ) or die $DBI::errstr;

open(FILEVAR,'NuestarExcelTicket.txt') || die "unable to open $!";
my $cnt = 0;
while(<FILEVAR>){
chomp $_;
$cnt++ ;
next if($cnt==1);
my @array_var = split ('\t',$_);
my ($create_date,@arr,$close_date,@arr1);
if ($array_var[3] ne ''){
	 @arr = split(' ',$array_var[3]);
	 $arr[0]=sprintf("%02d",(split(/\//,$arr[0]))[2])."-".sprintf("%02d",(split(/\//,$arr[0]))[0])."-".sprintf("%02d",(split(/\//,$arr[0]))[1]);
	 $create_date = "$arr[0] $arr[1]:00";
}
else{
	$create_date =	'0000-00-00 00:00:00';
}
if ($array_var[5] ne ''){
         @arr1 = split(' ',$array_var[5]);
         $arr1[0]=sprintf("%02d",(split(/\//,$arr1[0]))[2])."-".sprintf("%02d",(split(/\//,$arr1[0]))[0])."-".sprintf("%02d",(split(/\//,$arr1[0]))[1]);
         $close_date = "$arr1[0] $arr1[1]:00";
}
else{
        $close_date =  '0000-00-00 00:00:00';
}



     
my $query = "insert into neustarTicket (ticket_no,priority,createdBy,created_date,company,close_date,open,closed) values (?,?,?,?,?,?,?,?) ";
my $sth = $dbh->prepare($query);
$sth->execute($array_var[0],$array_var[1],$array_var[2],$create_date,$array_var[4],$close_date,$array_var[6],$array_var[7]);

print "$query====================\n";
}

