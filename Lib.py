#pgSQL設定，MySystem/pgDBSetting
pgSQLIP = system.tag.readBlocking('[edge]MySystem/pgDBSetting/pgSQLIP')[0].value
pgSQLPort = system.tag.readBlocking('[edge]MySystem/pgDBSetting/pgSQLPort')[0].value
pgSQLDB = system.tag.readBlocking('[edge]MySystem/pgDBSetting/pgSQLDB')[0].value
pgSQLuser = system.tag.readBlocking('[edge]MySystem/pgDBSetting/pgSQL_bkWorkUser')[0].value
pgSQLpassword = system.tag.readBlocking('[edge]MySystem/pgDBSetting/pgSQL_bkWorkPassword')[0].value

#取得本機IP
localip = system.net.getIpAddress()

#刪除此使用者/IP/閒置的SQLComm
resetSQLConn = "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state='idle' and client_addr = '" + localip + "' and usename = '" + pgSQLuser + "'"

#------------------Script Console---------------
"""
pgSQLLib.pgDBCreate()
"""
#-----------------------------------------------

def pgDBCreate():
	
	#pgSQL Connect and Create table
	props = Properties()
	props.put('user', pgSQLuser)
	props.put('password', pgSQLpassword)
	conn = Driver().connect("jdbc:postgresql://" + pgSQLIP + ":" + pgSQLPort + "/" + pgSQLDB + "", props)
	
	try:
		st = conn.prepareStatement("CREATE TABLE HisEvent(EventTime timestamp,State text ,DisplayPash text,Label text,EventValue text,CONSTRAINT hisevent_pkey PRIMARY KEY (eventtime, displaypash))")
		st.executeUpdate();
		
		st = conn.prepareStatement("CREATE INDEX "'"IDX_hisevent_eventtime"'" ON hisevent USING btree (eventtime ASC NULLS LAST)")
		st.executeUpdate();
		print "DB Create Table HisEvent OK..."
	except:
		print "DB Create Table HisEvent Error..."
	
	try:
		st = conn.prepareStatement("CREATE TABLE Fields(Id int,TagName text,TagType int,CONSTRAINT fields_pkey PRIMARY KEY (id))")
		st.executeUpdate();
		
		st = conn.prepareStatement("CREATE INDEX "'"IDX_fields_tagName"'" ON fields USING btree (tagname ASC NULLS LAST)")
		st.executeUpdate();
		print "DB Create Table Fields OK..."
	except:
		print "DB Create Table Fields Error..."
	
	try:
		st = conn.prepareStatement("CREATE TABLE Fields_partitions(pname text,Start_time timestamp,End_time timestamp)")
		st.executeUpdate();
		print "DB Create Table Fields_partitions OK..."
	except:
		print "DB Create Table Fields_partitions error..."
		
	try:
		st = conn.prepareStatement("CREATE TABLE Fields_Type(TypeID int,Type text)")
		st.executeUpdate();
			
		SQLComm = "INSERT INTO Fields_Type(TypeID, Type) VALUES "
		SQLComm += "(1,'Boolen'),"
		SQLComm += "(2,'Int'),"
		SQLComm += "(3,'Float')"
		st = conn.prepareStatement(SQLComm)
		st.executeUpdate();
		
		print "DB Create Table Fields_Type OK..."
	except:
		print "DB Create Table Fields_Type error..."

	
	#刪除此使用者/IP/閒置的SQL execute
	st = conn.createStatement()
	rs = st.executeQuery(resetSQLConn)
	
	rs.close()
	st.close()
	


#------------------Gateway Event Timer 15s(每15秒記錄一次)---------------
"""
pgSQLLib.pgDBInsertHisTagListValue()
"""
#-----------------------------------------------
def pgDBInsertHisTagListValue():
	#SOE
	Timestamp = system.date.now()


	#MySystem/HisTag/HisTagList先確定要存入的點位
	dt = system.tag.readBlocking('[edge]MySystem/HisTag/HisTaglist')[0].value
	
	
	#pgSQL Connect and Create table
	props = Properties()
	props.put('user', pgSQLuser)
	props.put('password', pgSQLpassword)
	conn = Driver().connect("jdbc:postgresql://" + pgSQLIP + ":" + pgSQLPort + "/" + pgSQLDB + "", props)
	
	#檢查fields_partitions是否已經有本月資料
	Year = system.date.getYear(system.date.now())
	Month = "%02d" % (system.date.getMonth(system.date.now()) + 1)
	CheckSQLComm = "select * from fields_partitions where pname = 'fields_data_" + str(Year) + "_" + str(Month) + "'"
	st = conn.createStatement();
	rs = st.executeQuery(CheckSQLComm)
	
	#fields_partitions如有資料則Insert，如無資料則先Create後再Insert
	value =""
	
	if rs.next():
		for i in range(0,dt.getRowCount()):
			if system.tag.readBlocking(dt.getValueAt(i,1))[0].value == True:
				TagValue = 1
			elif system.tag.readBlocking(dt.getValueAt(i,1))[0].value == False:
				TagValue = 0
			else:
				TagValue = system.tag.readBlocking(dt.getValueAt(i,1))[0].value

			value += "('{ID}','{TagName}','{Time}'),".format(ID=dt.getValueAt(i,0),TagName=TagValue,Time=Timestamp)
		
		SQLComm = "INSERT INTO fields_data_" + str(Year) + "_" + str(Month) +"(id, value, timestamp) VALUES "
		SQLComm += value[:-1]
			
		st = conn.prepareStatement(SQLComm)
		st.executeUpdate()
		print "DB INSERT Table fields_data_" + str(Year) + "_" + str(Month) + " OK..."
		
	else:
		SQLComm = "INSERT INTO fields_partitions(pname, start_time, end_time) VALUES "
		SQLComm += "('fields_data_" + str(Year) + "_" + str(Month) + "',date_trunc('month',current_date),date_trunc('month',current_date) + interval'1 month')"
		
		st = conn.prepareStatement(SQLComm)
		st.executeUpdate();
		
		st = conn.prepareStatement("CREATE TABLE Fields_data_" + str(Year) + "_" + str(Month) + "(Id int,Value Numeric,Timestamp timestamp,CONSTRAINT data_" + str(Year) + "_" + str(Month) + "_pkey PRIMARY KEY (id, "'"timestamp"'"))")
		st.executeUpdate();
		
		st = conn.prepareStatement("CREATE INDEX "'"IDX_data_' + str(Year) + '_' + str(Month) + '_timestamp"'" ON fields_data_" + str(Year) + "_" + str(Month) + " USING btree ("'"timestamp"'" ASC NULLS LAST)")
		st.executeUpdate();
		
		print "DB CREATE Table fields_data_" + str(Year) + "_" + str(Month) + " OK..."
		
		#-----------如無TABLE則先Create Table/Insert新的Partitlons--------------
		for i in range(0,dt.getRowCount()):
			if system.tag.readBlocking(dt.getValueAt(i,1))[0].value == True:
				TagValue = 1
			elif system.tag.readBlocking(dt.getValueAt(i,1))[0].value == False:
				TagValue = 0
			else:
				TagValue = system.tag.readBlocking(dt.getValueAt(i,1))[0].value

			value += "('{ID}','{TagName}','{Time}'),".format(ID=dt.getValueAt(i,0),TagName=TagValue,Time=Timestamp)
		
		SQLComm = "INSERT INTO fields_data_" + str(Year) + "_" + str(Month) +"(id, value, timestamp) VALUES "
		SQLComm += value[:-1]
			
		st = conn.prepareStatement(SQLComm)
		st.executeUpdate()
		print "DB INSERT Table fields_data_" + str(Year) + "_" + str(Month) + " OK..."


	#刪除此使用者/IP/閒置的SQL execute
	st = conn.createStatement()
	rs = st.executeQuery(resetSQLConn)
	
	rs.close()
	st.close()
	
	
	
#------------------Script Console---------------
"""
pgSQLLib.pgDBInsertHisTagList()
"""
#-----------------------------------------------
def pgDBInsertHisTagList():
	#MySystem/HisTag/HisTagList先確定要存入的點位
	dt = system.tag.readBlocking('[edge]MySystem/HisTag/HisTaglist')[0].value
	
	#設定pgSQL帳號密碼DB並Connect
	props = Properties()
	props.put('user', pgSQLuser)
	props.put('password', pgSQLpassword)
	conn = Driver().connect("jdbc:postgresql://" + pgSQLIP + ":" + pgSQLPort + "/"+ pgSQLDB , props)
	
	
	#先將fields Table全部刪除後，重新insert MySystem/HisTag/HisTagList的資料
	SQLComm = "delete from fields"
	st = conn.prepareStatement(SQLComm)
	st.executeUpdate();
	
	#先抓出Tag List內的數值Type    TagType => 1:bool  2:int  3:float
	paths = []
	tagtype = []
	
	for i in range(0,dt.getRowCount()):
		paths.append(dt.getValueAt(i,1))
	
	for i in range(0,dt.getRowCount()):
		tagvalue=type(system.tag.readBlocking(paths)[i].value)
		if tagvalue == bool:
			tagtype.append(1)
		elif tagvalue == int:
			tagtype.append(2)
		elif tagvalue == float:
			tagtype.append(3)
	
	#Sql insert comm
	value =""
	
	for i in range(0,dt.getRowCount()):
		value += "('{ID}','{TagName}','{TagType}'),".format(ID=dt.getValueAt(i,0),TagName=dt.getValueAt(i,1),TagType=tagtype[i])
	
	SQLComm = "INSERT INTO fields(id, tagname, tagtype) VALUES "
	SQLComm += value[:-1]
	
	st = conn.prepareStatement(SQLComm)
	st.executeUpdate();
	print "DB INSERT Table fields OK..."
	
	#刪除此使用者/IP/閒置的SQL execute
	st = conn.createStatement()
	rs = st.executeQuery(resetSQLConn)
	
	rs.close()
	st.close()


#------------------Gateway Events TagChange(Day)---------------
"""
#刪除6個月前資料
pgSQLLib.pgDBDeleteHisData(6)
"""
#-----------------------------------------------
def pgDBDeleteHisData(HistoryMonth):

	#設定pgSQL帳號密碼DB並Connect
	props = Properties()
	props.put('user', pgSQLuser)
	props.put('password', pgSQLpassword)
	conn = Driver().connect("jdbc:postgresql://" + pgSQLIP + ":" + pgSQLPort + "/"+ pgSQLDB , props)
	
	#刪除HistoryDay {HistoryDay} 前資料
	SQLComm = "delete from hisevent where eventtime <= date_trunc('Second',now()::timestamp) - interval '" + str(HistoryMonth) + " Month'"
	st = conn.prepareStatement(SQLComm)
	st.executeUpdate();
	
	#刪除Fields_data_ {HistoryDay} 前資料 / Table
	try:
		hisDate = system.date.addMonths(system.date.now() , 0-HistoryMonth)
		hisDateYear = system.date.format(hisDate,"yyyy")
		hisDateMon = system.date.format(hisDate,"MM")
		
		SQLComm = "delete from fields_data_" + hisDateYear + "_" + hisDateMon + " where timestamp <= date_trunc('Second',now()::timestamp) - interval '" + str(HistoryMonth) + " Month'"
		st = conn.prepareStatement(SQLComm)
		st.executeUpdate();
		
		print "DB delete 'fields_data_" + hisDateYear + "_" + hisDateMon + "' data OK..."
		
	except:
		print "DB can't find 'fields_data_" + hisDateYear + "_" + hisDateMon + "' Table!!"
		
		
	#刪除Fields_data_ {HistoryDay} 前Table
	try:
		hisDate = system.date.addMonths(system.date.now() , -1-HistoryMonth)
		hisDateYear = system.date.format(hisDate,"yyyy")
		hisDateMon = system.date.format(hisDate,"MM")
		
		SQLComm = "delete from fields_partitions where pname = " + "'fields_data_" + hisDateYear + "_" + str(hisDateMon) + "'"
		st = conn.prepareStatement(SQLComm)
		st.executeUpdate();
		print "DB delete fields_partitions's 'fields_data_" + hisDateYear + "_" + hisDateMon + "' data OK..."
		
		SQLComm = "drop table fields_data_" + hisDateYear + "_" + str(hisDateMon)
		st = conn.prepareStatement(SQLComm)
		st.executeUpdate();
		print "DB drop 'fields_data_" + hisDateYear + "_" + hisDateMon + "' data OK..."
		
	except:
		print "DB can't find 'fields_data_" + hisDateYear + "_" + hisDateMon + "' Table!!"


	#刪除此使用者/IP/閒置的SQL execute
	st = conn.createStatement()
	rs = st.executeQuery(resetSQLConn)	

	rs.close()
	st.close()

#------------------Gateway Events Timer 5S---------------
"""
#每5秒記錄一次即時警報資料進DB
pgSQLLib.pgDBInsertHisevent(5)
"""
#-----------------------------------------------
def pgDBInsertHisevent(PulseSecond):

	#設定pgSQL帳號密碼DB並Connect
	props = Properties()
	props.put('user', pgSQLuser)
	props.put('password', pgSQLpassword)
	conn = Driver().connect("jdbc:postgresql://" + pgSQLIP + ":" + pgSQLPort + "/"+ pgSQLDB , props)
	
	
	#配合使用getRealAlarm(Second)回撈5秒即時警報，可放入Client Script Timer 60s
	realalarm = RWDataLib.getRealAlarm(PulseSecond)
	
	alarmTotal = len(realalarm)
	
	AlmEventTime = str(realalarm[0])
	AlmDisplayPathOrSource = str(realalarm[1])
	AlmLabel = str(realalarm[2])
	AlmeventValue = str(realalarm[3])
	
	
	if AlmEventTime <> 'none':
		value =""
		
		for i in range(alarmTotal/5):
		
			AlmEventTime = str(realalarm[(i*5)])
			AlmState = str(realalarm[(i*5)+1])
			AlmDisplayPathOrSource = str(realalarm[(i*5)+2])
			AlmLabel = str(realalarm[(i*5)+3])
			AlmeventValue = str(realalarm[(i*5)+4])
			
			value += "('{AlmEventTime}','{AlmState}','{AlmDisplayPathOrSource}','{AlmLabel}','{AlmeventValue}')," \
			.format( \
			AlmEventTime=AlmEventTime, \
			AlmState=AlmState, \
			AlmDisplayPathOrSource=AlmDisplayPathOrSource, \
			AlmLabel=AlmLabel, \
			AlmeventValue=AlmeventValue)
			
		SQLComm = "INSERT INTO hisevent(eventtime, state, displaypash, label, eventvalue) VALUES "
		SQLComm += value[:-1]
	
		st = conn.prepareStatement(SQLComm)
		st.executeUpdate();
		print "DB INSERT Table hisevent OK..."
	
	#刪除此使用者/IP/閒置的SQL execute
	st = conn.createStatement()
	rs = st.executeQuery(resetSQLConn)			

	rs.close()
	st.close()
	
