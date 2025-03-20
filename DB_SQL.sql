CREATE TABLE ip_tbl
(
ip VARCHAR2(32),
old_ip VARCHAR2(32),
linked_accounts VARCHAR2(25),
PRIMARY KEY(ip)
);

CREATE TABLE gammertag_tbl
(
gamertag VARCHAR2(25),
platform VARCHAR2(10),
ip VARCHAR2(32),
clan VARCHAR2(25),
clan_role VARCHAR2(25),
old_clan VARCHAR2(25),
PRIMARY KEY(gamertag),
FOREIGN KEY (ip) REFERENCES ip_tbl(ip),
FOREIGN KEY (clan) REFERENCES clan_tbl(clan)
FOREIGN KEY (old_clan) REFERENCES clan_tbl(clan) 
);

CREATE TABLE clan_tbl
(
clan VARCHAR2(25),
active_status VARCHAR2(10),
relations VARCHAR2(0),
threat_level VARCHAR2(10),
PRIMARY KEY(clan)
);

INSERT INTO ip_tbl VALUES ('1239823983938293829','3219823983938293829','davechapel');
INSERT INTO ip_tbl VALUES ('5639823983938293829','N/A','McDobby69');
INSERT INTO ip_tbl VALUES ('9239823983938293829','N/A','PizzaBoy14 , mike_hunt911');


INSERT INTO gamertag_tbl VALUES ('davechapel','XBL','1239823983938293829','KKK','grand wizard','N/A');
INSERT INTO gamertag_tbl VALUES ('McDobby69','PC & XBL','5639823983938293829','N/A','N/A','N/A');
INSERT INTO gamertag_tbl VALUES ('PizzaBoy14','PSN','9239823983938293829','SS','Oberleutnant','SA');
INSERT INTO gamertag_tbl VALUES ('mike_hunt911','PC','9239823983938293829','SS','Associate','N/A');


INSERT INTO clan_tbl VALUES ('KKK','Active','Good','Low');
INSERT INTO clan_tbl VALUES ('SS','Low','Medium','High');
INSERT INTO clan_tbl VALUES ('SA','Dead','Medium','High');


COMMIT;