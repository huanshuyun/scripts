# -*- coding:utf-8 -*-
import os
import re
import pypyodbc
import time
import subprocess
from datetime import datetime
from pprint import pprint
import xml.etree.ElementTree as ET
"""
整个内容分为两个类：
Access类：用来连接数据库的模块
Tenant类：包含了很多和租户日常业务有关的方法。

在日常业务中，会开发很多自动化的脚本，通过调用对象的方式使用此脚本中各种方法。这样就会简化很多其他业务的脚本，对于后期的使用和修改提供了很大的方便。
"""
#连接数据库的类
class Access(object):
    def __init__(self,ServerIP,database = db,user = user,password = password):
        #super(Access, self).__init__()
        self.ServerIP = ServerIP
        self.database = database
        self.__user = user
        self.__password = password
        self.con = self.connect()
        self.cursor = self.con.cursor()
        self.result = None
    #析构函数，用来回收未关闭的数据库链接数
    def __del__(self):
        if self.cursor:
            self.cursor.close()
        if self.con:
            self.con.close()
    #连接数据库方法
    def connect(self):
        try:
            con = pypyodbc.connect("DRIVER={SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s" % (self.ServerIP,self.database,self.__user,self.__password),autocommit=True)
        except Exception, e:
            raise e
        return con
    #执行数据库语句
    def execute(self, sql, *args):
        try:
            self.cursor.execute(sql, args)
            self.result = self.cursor.fetchall()
        except pypyodbc.ProgrammingError, e:
            self.result = None
        except Exception, e:
            raise e
        while self.cursor.nextset():
            pass
        return self.result
            
#有关租户信息查询和日常业务重复用到的方法整理
class Tenant(Access):
    """docstring for ClassName"""
    def __init__(self, DatabaseName):
        self.DatabaseName = DatabaseName
        self.MCIP = self.TenantInfo()[0][0]
        self.MCName = self.TenantInfo()[0][1]
        self.AppIP = self.TenantInfo()[0][2]
        self.DatabaseID = self.TenantInfo()[0][3]
        self.DatabaseIP = self.TenantInfo()[0][4]
        self.DomainName = self.TenantInfo()[0][5]
        self.TenantName = self.TenantInfo()[0][6]
        self.DisplayName = self.TenantInfo()[0][7]
        self.CreateDate = self.TenantInfo()[0][8]
        self.Version = self.TenantInfo()[0][9]
        self.TenantType = self.TenantInfo()[0][10]
        self.DomainPrefix = self.DomainName.split(".")[0]
        self.new_MCName = self.get_NEWMC_info()[0][0]
        self.new_MCIP = self.get_NEWMC_info()[0][1]
        Access.__init__(self,self.DatabaseIP)
        self.TenantID = self.GetTenantID()[0][0]
        self.lic_tenant = self.Getlic()[0]
        self.lic_tenant_l = self.Getlic()[1]

    def TenantInfo(self):
        try:
            SQL = "select MCIP,MCName,AppIP,DatabaseID,DatabaseIP,DomainName,TenantName,DisplayName,CreateDate,Version,TenantType from T_TenantInfoFromMC where DatabaseName = '{}'".format(self.DatabaseName)
            con = pypyodbc.connect("DRIVER={SQL Server};SERVER=10.129.3.51;DATABASE=OMP;UID=dbuser;PWD=saKingdee$2200",autocommit=True)
            cursor = con.cursor()
            cursor.execute(SQL)
            result = cursor.fetchall()
            while cursor.nextset():
                pass
            cursor.close()
            con.close()
            return result
        except Exception, e:
            raise e

    def GetTenantID(self):
        try:
            GT = Access(self.MCIP,self.MCName)
            GetTenantIDSql = "select FTENANTID from T_BAS_DATACENTER where FDATABASENAME = '{}'".format(self.DatabaseName)
            TenantID = GT.execute(GetTenantIDSql)
            return TenantID
        except Exception, e:
            raise e

    def Getlic(self):
        try:
            GL = Access(self.MCIP,self.MCName)
            GetLicSql = [
                # "select * from T_BAS_TENANT where FHOST = '{}'".format(DomainName), 
                "select * from T_BAS_TENANT where FID = '{}'",
                "select * from T_BAS_TENANT_L where FID = '{}'"
                ]
            lic_tenant=''
            lic_tenant_l=''
            if self.TenantID !='':
                lic_tenant = GL.execute(GetLicSql[0].format(self.TenantID))
                lic_tenant_l = GL.execute(GetLicSql[1].format(self.TenantID))
            return lic_tenant,lic_tenant_l
        except Exception, e:
            raise e

    def Backup(self,BackupFileName):
        try:
            #Now = time.strftime('%Y%m%d%H%M%S')
            #BackupFileName = "\\\\10.130.6.16\Patch\\" + self.DatabaseName + ".bak"
            #BackupFileName = "f:\\backup\\" + {} + '_' + Now + ".bak" .format(self.DatabaseName)
            backupsql = "backup database %s to disk = '%s' WITH FORMAT, COMPRESSION, INIT" % (self.DatabaseName,BackupFileName)
            self.execute(backupsql)
        except Exception, e:
            raise e
        return BackupFileName
        
    def Restore(self,BakFile):
        try:
            print BakFile
            ResSql = "exec p_RestoreK3DB '%s','%s'" % (self.DatabaseName,BakFile)
            self.execute(ResSql)
        except Exception, e:
            raise e

#==============================发邮件
    def sendmail(self,text,receivers):
        import smtplib
        from email.mime.text import MIMEText
        from email.header import Header

        sender = "3010492291@qq.com"
        message = MIMEText(text,'plain','utf-8')
        message['From'] = Header('this is sender','utf-8')
        message['To'] = Header('this is receiver','utf-8')
        
        subject = 'Result'
        message['Subject'] = Header(subject,'utf-8')
        
        try:
            smtpObj = smtplib.SMTP()
            smtpObj.connect('smtp.qq.com', 25)
            smtpObj.starttls()
            smtpObj.login(sender,'gajidsknnrdgdfeb')

            smtpObj.sendmail(sender, receivers, message.as_string())
            smtpObj.quit()
            return "%s Send mail success." %text
        except smtplib.SMTPException,e:
            return  e.message

#==============================租户迁移
    def get_NEWMC_info(self):
        try:
            if os.path.exists('D:\Program Files (x86)\Kingdee\K3Cloud\ManageSite'):
                    config_file = r'D:\Program Files (x86)\Kingdee\K3Cloud\ManageSite\App_Data\Common.config'
                    content = None
                    with open(config_file, 'r') as f:
                        content = f.read()
                    # mc 可配置多个，通过enabled参数指定，正则匹配有待改进
                    new_MC_info = re.findall('DatabaseEntity="(.*?)"\s.*\sDbServerInstance="(.*?)"', content, re.S)
            else:
                new_MC_info = (['',''],)
            return new_MC_info
        except Exception, e:
            return "get_NEWMC_info failed!"

    def copy_lic(self,MCName,MCIP):
        insert_lic_sql = [
                "insert into T_BAS_TENANT values({})".format(','.join(["?"] * len(self.lic_tenant[0]))),
                "insert into T_BAS_TENANT_L values({})".format(','.join(["?"] * len(self.lic_tenant_l[0])))
                ]
        Search = Access(MCIP,MCName)
        has_lic = int(Search.execute("select count(1) from t_bas_tenant where fid = '{}'".format(self.TenantID))[0][0])
        try:
            if has_lic:    
                Search.execute("delete from t_bas_tenant where fid = '{}'".format(self.TenantID))
                Search.execute("delete from t_bas_tenant_l where fid = '{}'".format(self.TenantID))
            Search.execute(insert_lic_sql[0], *self.lic_tenant[0])
            Search.execute(insert_lic_sql[1], *self.lic_tenant_l[0])
        except Exception, e:
            return "copy lic failed!"

    def unRegUser(self,MCName,MCIP,DatabaseName):
        try:
            unreg = Access(MCIP)
            unreg.execute("""exec p_UnRegUser '{}', '{}' """.format(MCName, DatabaseName))
        except Exception, e:
            return "unRegUser failed!"

    def RegUser(self,MCIP,MCName):
        try:
            reg = Access(MCIP)
            reg.execute("exec p_RegUser20 '{}', '{}', '{}', '{}', '{}','{}'".format(MCName, self.DatabaseName, self.DisplayName, self.DomainPrefix, self.DatabaseIP, self.DatabaseID))
        except Exception, e:
            return "RegUser failed!"

    def update_mobile_settings(self,datacenter_id, MCIP, MCName):
        try:
            mobile = Access(MCIP,MCName)
            result = mobile.execute("select FPARAMETERS from T_BAS_USERPARAMETER where FUSERID=-1 and FPARAMETEROBJID='AccId'")
            if len(result[0]) == 0:
                mobile.execute("insert into T_BAS_USERPARAMETER values(newid(), 'UserParameter', -1, 'AccId', CONVERT(xml, '<Root>{}</Root>'))".format(datacenter_id))
            else:
                tree = ET.fromstring(result[0][0])
                if tree.text is None:   # the old xml settings is "<Root />"
                    new_xml_str = '<Root>{}</Root>'.format(datacenter_id)  
                else:       # the old xml settings is like "<Root>xxx</Root>" or "<Root>xxx,yyy</Root>"
                    new_xml_str = "<Root>{},{}</Root>".format(tree.text, datacenter_id)

                mobile.execute("Update T_BAS_USERPARAMETER set FPARAMETERS=CONVERT(xml, '{}') where FUSERID=-1 and FPARAMETEROBJID='AccId'".format(new_xml_str))
        except Exception, e:
            return "update_mobile_settings failed!"

    def migration(self,appip):
        text = ''
        #copy licences
        try:
            result = self.copy_lic(self.new_MCName,self.new_MCIP)
        except Exception, e:
            text = text + result
            return text
        # unreg
        try:
            result = self.unRegUser(self.MCName,self.MCIP,self.DatabaseName)
        except Exception, e:
            text = text + result
            return text
        # reg
        try:
            result = self.RegUser(self.new_MCIP, self.new_MCName)
        except Exception, e:
            text = text + result
            return text
        # update tenant info from mc
        try:
            update_tenant = Access('10.129.3.51','OMP')
            update_tenant_SQL = "update T_TenantInfoFromMC set MCName = '{}', MCIP = '{}', AppIP = '{}' where DatabaseName = '{}'".format(self.new_MCName, self.new_MCIP, appip, self.DatabaseName)
            update_tenant.execute(update_tenant_SQL)
        except Exception, e:
            text = "update_tenant failed!"
            return text

        # update mobile settings
        if not ("TMP" in self.DatabaseName or "TEST" in self.DatabaseName):
            #print "updating mobile settings"
            try:
                mobile = Access(self.MCIP,self.MCName)
                result = mobile.execute("select FPARAMETERS from T_BAS_USERPARAMETER where FUSERID=-1 and FPARAMETEROBJID='AccId'")
                if len(result[0]) == 0:    # no settings
                    return
                xml_str = result[0][0]
                tree = ET.fromstring(xml_str)
                datacenter_id_list = tree.text.split(',') if tree.text is not None else [] 
                if self.DatabaseID not in datacenter_id_list:   # the mobile settings doesn't enabled
                    return
                result = self.update_mobile_settings(self.DatabaseID,self.new_MCIP,self.new_MCName)
            except Exception, e:
                text = text + result 
                return text