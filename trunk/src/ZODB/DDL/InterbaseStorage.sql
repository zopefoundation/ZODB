/* Replace the CREATE DATABASE path argument with
the path and file you want to use for your
InterbaseStorage

$Id: InterbaseStorage.sql,v 1.1 2000/05/27 08:09:58 chrism Exp $
*/

CREATE DATABASE '/home/chrism/zope_storage2.gdb';

create table zodb_data (
	z_oid		char(12) not null,
	z_serial	char(12) not null,
	z_prev		char(12),
	z_status	char(1),
	z_data		blob,
	z_datalen	integer,
	z_dataserial	char(12),		
	primary key (z_oid, z_serial)
);
 
create table zodb_trans (
	z_serial	char(12) not null primary key,
	z_status	char(1),
	z_username	varchar(255),
	z_description	varchar(255),
	z_ext		varchar(255)
);

create table zodb_version (
       	z_version	varchar(255),
	z_oid		char(12) not null,
	z_serial	char(12) not null,
	z_status	char(1),
	z_nvserial	char(12),
	primary key (z_oid, z_serial)
);
