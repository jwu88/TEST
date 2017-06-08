using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Web.Caching;
using System.Text;
using System.Text.RegularExpressions;
using Falcon.Models;
using System.Configuration;

namespace Falcon.DataAccess
{
    public class FalconDbRepository<T>
    {
        private PetaPoco.Database dbFalcon = new PetaPoco.Database("FalconDb");
        private PetaPoco.Database dbFalconUI = new PetaPoco.Database("FalconUIDb");

        private string query;
        private string sessionName;

        #region charts
        public FalconDbRepository(string reportID = null, string reportName = null)
        {
            try
            {
                if (!string.IsNullOrWhiteSpace(reportID))
                {
                    ReportQuery reportQuery = new ReportQuery();

                    var psbQuery = PetaPoco.Sql.Builder
                    .Select("Query, SessionName")
                    .From("FalconReports")
                    .Where("ReportId = @0", reportID);

                    try
                    {
                        reportQuery = dbFalconUI.FirstOrDefault<ReportQuery>(psbQuery);
                        query = reportQuery.Query;
                        sessionName = reportQuery.SessionName;
                    }
                    catch { }

                    switch (reportID)
                    {
                        case "AMReport01":
                            query = ";exec spVirtualVPhysicalServers";
                            sessionName = "AMReport1Data";
                            break;
                        case "AMReport02":
                            query = "select * from vwOperatingSystemCounts order by ServerCount";
                            sessionName = "AMReport2Data";
                            break;
                        case "AMReport03":
                            query = BuildReport03Query();
                            sessionName = "AMReport3Data";
                            break;
                        case "AMReport05":
                            query = "select * from vwVirtualServerFunctionByLocation order by FUNC";
                            sessionName = "AMReport5Data";
                            break;
                        case "AMReport07":
                            query = "select * from vwVirtualServerTypeByProcCount order by ServerFunction";
                            sessionName = "AMReport7Data";
                            break;
                        case "AMReport07DD":
                            query = "select * from vwVirtualServerTypeByProcCountDD order by ServerFunction";
                            sessionName = "AMReport7DDData";
                            break;
                        case "AMReport08":
                            query = "select * from vwVirtualServerTypeByMemory order by ServerFunction";
                            sessionName = "AMReport8Data";
                            break;
                        case "AMReport08DD":
                            query = BuildReport08DDQuery();
                            sessionName = "AMReport8DDData";
                            break;
                        case "AMReport09":
                            query = BuildReport09Query();
                            sessionName = "AMReport9Data";
                            break;
                        case "AMReport10":
                            query = "select * from vwDatabaseInstanceCounts";
                            sessionName = "AMReport10Data";
                            break;
                        case "AMReport11":
                            query = BuildReport11Query().ToString();
                            sessionName = "AMReport11Data";
                            break;
                        case "AMReport12":
                            query = "select * from vwDatabaseSizeByType";
                            sessionName = "AMReport12Data";
                            break;
                        case "AMReport13":
                            query = "select * from vwOperatingSystemEOL order by EndOfLifeDT";
                            sessionName = "AMReport13Data";
                            break;
                        case "AMReport14":
                            query = BuildReport14Query();
                            sessionName = "AMReport14Data";
                            break;
                        case "AMReport15":
                            query = BuildReport15Query();
                            sessionName = "AMReport15Data";
                            break;
                        case "AMReport16":
                            query = BuildReport16Query();
                            sessionName = "AMReport16Data";
                            break;
                        case "AMReport17":
                            query = BuildReport17Query();
                            sessionName = "AMReport17Data";
                            break;
                        case "AMReport18":
                            query = BuildReport18Query();
                            sessionName = "AMReport18Data";
                            break;
                        case "AMReport19":
                            query = BuildReport19Query();
                            sessionName = "AMReport19Data";
                            break;
                        case "OverviewDatabasesCount":
                            query = "select Category, HDC, FDC, AWSUS, AWSEU, GrandTotal, DisplayOrder from vwDBInstanceOverview order by DisplayOrder";
                            sessionName = "DatabasesCountData";
                            break;
                        case "OverviewDatabaseServerCount":
                            query = "select Category, HDC, FDC, AWSUS, AWSEU, GrandTotal, DisplayOrder from vwDBServerOverview order by DisplayOrder";
                            sessionName = "DatabasesServerCountData";
                            break;
                        case "OverviewOSCount":
                            query = "select Category, HDC, FDC, AWSUS, AWSEU, GrandTotal, DisplayOrder from vwOSOverview order by DisplayOrder";
                            sessionName = "OSCountData";
                            break;
                        case "OverviewProductCount":
                            query = "select Category, HDC, FDC, AWSUS, AWSEU, GrandTotal, DisplayOrder from vwProductOverview order by DisplayOrder";
                            sessionName = "ProductCountData";
                            break;
                        case "AMReport20":
                            query = BuildReport20Query();
                            sessionName = "AMReport20Data";
                            break;
                        case "AMReport21":
                            query = BuildReport21Query();
                            sessionName = "AMReport21Data";
                            break;
                        case "EnvReport":
                            query = BuildEnvReportQuery(reportName);
                            sessionName = "none";
                            break;
                        case "AMReport31":
                            query = BuildReport31Query();
                            sessionName = "AMReport31Data";
                            break;
                        case "AMReport32":
                            query = BuildReport32Query();
                            sessionName = "AMReport32Data";
                            break;
                        case "AMReport34":
                            query = BuildReport34Query();
                            sessionName = "AMReport34Data";
                            break;
                    }
                }
            }
            catch { throw; }
        }

        public T GetReportData()
        {
            try
            {
                T reportData = default(T);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    reportData = dbFalcon.First<T>(query);
                    System.Web.HttpContext.Current.Session[sessionName] = reportData;
                }
                else
                    reportData = (T)System.Web.HttpContext.Current.Session[sessionName];

                return reportData;
            }
            catch { throw; }
        }

        public List<T> GetReportDataList()
        {
            try
            {
                List<T> reportData = null;

                if (System.Web.HttpContext.Current.Session[sessionName] == null || sessionName == "none")
                {
                    reportData = dbFalcon.Fetch<T>(query);
                    System.Web.HttpContext.Current.Session[sessionName] = reportData;
                }
                else
                    reportData = (List<T>)System.Web.HttpContext.Current.Session[sessionName];

                return reportData;
            }
            catch { throw; }
        }
        #endregion

        #region drill down server list
        public List<T> GetServerDDList(string reportName, string location, string type, int? procCount, decimal? memory, string model)
        {
            try
            {
                List<T> result;
                sessionName = string.Format("{0}{1}{2}{3}{4}{5}", reportName, location, type, procCount, memory, model);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Append("select ServerID, ServerName, OSName, ClientName, URL, MemoryMB, ProcessorCount, Model")
                        .Append("from vwServerListDDCommon slc")
                        .Append("where ServerID in (")
                        .Append("select sf.ServerID")
                        .Append("from vwServerFunction sf")
                        .Append("inner join vwServerHardware s on sf.ServerID = s.ServerID");
                    //.Append("and sf.DeviceType is not null");

                    // all report03
                    if (reportName.Contains("AMReport03") && type == null && procCount == null && memory == null && model == null)
                    {
                        query.Append("and s.Virtual = 0");

                        if (location != null)
                            query.Append("and sf.LocationCD = @0", location);
                    }
                    // all report05
                    else if (reportName.Contains("AMReport05") && type == null && procCount == null && memory == null && model == null)
                    {
                        query.Append("and s.Virtual = 1");

                        if (location != null)
                            query.Append("and sf.LocationCD = @0", location);
                    }
                    else
                    {
                        if (location != null)
                            query.Append("and sf.LocationCD = @0", location);
                        if (type != null)
                            query.Append("and sf.[Function] = @0 and slc.Virtual = 1", type);
                        if (procCount != null)
                            query.Append("and s.ProcessorCount = @0 and slc.Virtual = 1", procCount);
                        if (memory != null)
                            query.Append("and s.MemoryGB = @0 and slc.Virtual = 1", memory);
                        if (model != null)
                            query.Append("and s.Model = @0 and s.Virtual = 0", model);
                    }

                    query.Append(")");

                    result = dbFalcon.Fetch<T>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<T>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public List<T> GetServerDDList(string reportName, string location, string virt, string os)
        {
            try
            {
                List<T> result;
                sessionName = string.Format("{0}{1}{2}{3}", reportName, location, virt, os);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    if (virt != null)
                    {
                        if (virt == "Virtual")
                            virt = "Yes";
                        else
                            virt = "No";
                    }

                    var query = PetaPoco.Sql.Builder
                        .Append("select ServerID, ServerName, OSName, ClientName, URL, MemoryMB, ProcessorCount, Model")
                        .Append("from vwServerListDDCommon slc")
                        .Append("where ServerID in ")
                        .Append("(")
                        .Append("select ServerID")
                        .Append("from vwServerListOS os");

                    // all
                    if (location != null && virt == null && os == null)
                        query.Append("where os.Location = @0", location);

                    // report01
                    if (virt != null && os == null)
                    {
                        if (location != null)
                        {
                            query.Append("where os.Location = @0", location)
                                .Append("and os.Virtual = @0", virt);

                        }
                        else
                            query.Append("where os.Virtual = @0", virt);
                    }

                    // report02
                    if (os != null)
                    {
                        if (location != null)
                        {
                            query.Append("where os.Location = @0", location)
                                .Append("and os.OS = @0", os)
                                .Append("and os.Virtual = @0", virt);
                        }
                        else
                            query.Append("where os.OS = @0", os)
                                .Append("and os.Virtual = @0", virt);
                    }

                    query.Append(")");

                    result = dbFalcon.Fetch<T>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<T>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public List<T> GetServerDDListDb(string reportName, string location, string virt, string db, string version, string reportType, string serverName)
        {
            try
            {
                List<T> result;
                sessionName = string.Format("{0}{1}{2}{3}{4}{5}{6}", reportName, location, virt, db, version, reportType, serverName);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    result = dbFalcon.Fetch<T>(";exec spServerDDDb @0, @1, @2, @3, @4, @5",
                                                             location, virt, db, version, reportType, serverName);
                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<T>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }
        #endregion

        #region report queries
        private string BuildReport03Query()
        {
            try
            {
                StringBuilder query = new StringBuilder()
                    .Append("select Model, LocationCD as Location, count(ServerName) as ServerCount ")
                    .Append("from vwServerHardware ")
                    .Append("where Virtual = 0 ")
                    //.Append("and Model not like 'VMWare%' and Model not like 'AWS%' and Model != 'Unknown'")
                    .Append("group by Model, LocationCD")
                    ;

                return query.ToString();
            }
            catch { throw; }
        }

        private string BuildReport11Query()
        {
            try
            {
                StringBuilder query = new StringBuilder()
                    .Append("select Location, Virtual, Name, sum(SizeMB) SizeMB ")
                    .Append("from vwDatabaseSizes ")
                    .Append("group by Location, Virtual, Name")
                    ;

                return query.ToString();
            }
            catch { throw; }
        }

        private string BuildReport08DDQuery()
        {
            try
            {
                StringBuilder query = new StringBuilder()
                    .Append("select ServerFunction, HDC, FDC, AWS, count(ServerFunction) as ServerCount ")
                    .Append("from vwVirtualServerTypeByMemoryDD ")
                    .Append("group by ServerFunction, HDC, FDC, AWS ")
                    .Append("order by ServerFunction")
                    ;

                return query.ToString();
            }
            catch { throw; }
        }

        private string BuildReport09Query()
        {
            try
            {
                StringBuilder query = new StringBuilder()
                    //.Append("select Location, Virtual, VersionDescription, Version, sum(InstanceCount) InstanceCount ")
                    //.Append("from vwDatabaseInstanceCounts ")
                    .Append("select Location, Virtual, VersionDescription, Version, count(DBName) InstanceCount ")
                    .Append("from vwServerDatabaseDetail ")
                    .Append("where VersionDescription like 'SQL Server%' ")
                    .Append("group by Location, Virtual, VersionDescription, Version ")
                    .Append("order by VersionDescription, Version")
                    ;

                return query.ToString();
            }
            catch { throw; }
        }

        private string BuildReport14Query()
        {
            try
            {
                StringBuilder query = new StringBuilder()
                    .Append("select Environment, RaveVersion, RaveURLCnt ")
                    .Append("from vwRaveURLEOL ")
                    .Append("order by Environment, RaveVersion")
                    ;

                return query.ToString();
            }
            catch { throw; }
        }

        private string BuildReport15Query()
        {
            try
            {
                StringBuilder query = new StringBuilder()
                    .Append("select OSName, OSVersion, ServerType, EndOfLifeDT, count(RaveURL) RaveCount ")
                    .Append("from vwRaveEOLDD ")
                    .Append("where EndOfLifeInd = 1 ")
                    .Append("group by OSName, OSVersion, ServerType, EndOfLifeDT order by Environment, OSName")
                    ;

                return query.ToString();
            }
            catch { throw; }
        }

        private string BuildReport16Query()
        {
            try
            {
                StringBuilder query = new StringBuilder()
                    .Append("select OSName, EndOfLifeDT, clientname as ClientName, RaveURL, RaveVersion, count(RaveURL) as CountOfURL ")
                    .Append("from vwRaveEOLDD ")
                    .Append("where EndOfLifeInd = 1 ")
                    .Append("group by OSName, EndOfLifeDT, clientname, RaveURL, RaveVersion")
                    ;

                return query.ToString();
            }
            catch { throw; }
        }

        private string BuildReport17Query()
        {
            try
            {
                StringBuilder query = new StringBuilder()
                    //.Append("select Location, Virtual, VersionDescription, Version, sum(ServerCount) ServerCount ")
                    //.Append("from vwDatabaseServerCounts ")
                    .Append("select Location, Virtual, VersionDescription, Version, count(distinct ServerName) ServerCount ")
                    .Append("from vwServerDatabaseDetail ")
                    .Append("where VersionDescription like 'SQL Server%' ")
                    .Append("group by Location, Virtual, VersionDescription, Version ")
                    .Append("order by VersionDescription, Version")
                    ;

                return query.ToString();
            }
            catch { throw; }
        }

        private string BuildReport18Query()
        {
            try
            {
                StringBuilder query = new StringBuilder()
                    .Append("select Location, Virtual, VersionDescription, sum(InstanceCount) InstanceCount ")
                    .Append("from vwDatabaseInstanceCounts ")
                    .Append("group by Location, Virtual, VersionDescription")
                    ;

                return query.ToString();
            }
            catch { throw; }
        }

        private string BuildReport19Query()
        {
            try
            {
                StringBuilder query = new StringBuilder()
                    .Append("select Location, Virtual, VersionDescription, sum(ServerCount) ServerCount ")
                    .Append("from vwDatabaseServerCounts ")
                    .Append("group by Location, Virtual, VersionDescription")
                    ;

                return query.ToString();
            }
            catch { throw; }
        }

        private string BuildReport20Query()
        {
            try
            {
                StringBuilder query = new StringBuilder()
                    .Append("select datacenter, urltype, count(urlType) totalUrls ")
                    .Append("from vwSiteInfo ")
                    .Append("group by datacenter, urltype")
                    ;

                return query.ToString();
            }
            catch { throw; }
        }

        private string BuildReport21Query()
        {
            try
            {
                StringBuilder query = new StringBuilder()
                    .Append("select datacenter, RaveVersion, count(url) totalUrls ")
                    .Append("from vwSiteInfo ")
                    .Append("group by datacenter, RaveVersion ")
                    .Append("order by datacenter, RaveVersion")
                    ;

                return query.ToString();
            }
            catch { throw; }
        }

        private string BuildEnvReportQuery(string name)
        {
            try
            {
                StringBuilder query = new StringBuilder()
                    .Append("select Id, EnvName, EnvURL, count(Client) as Total ")
                    .Append("from vwEnvironmentList ")
                    .Append("where EnvApplication = '" + name + "' ")
                    .Append("and Client != 'NA' ")
                    .Append("group by Id, EnvName, EnvURL ")
                    .Append("union ")
                    .Append("select Id, EnvName, EnvURL, 0 as Total ")
                    .Append("from vwEnvironmentList ")
                    .Append("where EnvApplication = '" + name + "' ")
                    .Append("and Client = 'NA' ")
                    .Append("group by Id, EnvName, EnvURL")
                    ;

                return query.ToString();
            }
            catch { throw; }
        }

        private string BuildReport31Query()
        {
            try
            {
                StringBuilder query = new StringBuilder()
                    .Append("select VCenter, HostCount, GuestCount  ")
                    .Append("from vwVMSummary")
                    ;

                return query.ToString();
            }
            catch { throw; }
        }

        private string BuildReport32Query()
        {
            try
            {
                StringBuilder query = new StringBuilder()
                    .Append("select *  ")
                    .Append("from vwVMHost")
                    ;

                return query.ToString();
            }
            catch { throw; }
        }

        private string BuildReport34Query()
        {
            try
            {
                StringBuilder query = new StringBuilder()
                    .Append("select *  ")
                    .Append("from vwVMHostCapacity ")
                    .Append("order by HostCount")
                    ;

                return query.ToString();
            }
            catch { throw; }
        }

        #endregion
    }

    public class FalconDbRepository : Controller
    {
        private PetaPoco.Database dbFalcon = new PetaPoco.Database("FalconDb");
        private PetaPoco.Database dbFalconUI = new PetaPoco.Database("FalconUIDb");
        private PetaPoco.Database dbCMPTracker = new PetaPoco.Database("CMPTrackerDb");

        private string sessionName;

        public List<Application> GetAppplicationList()
        {
            List<Application> result;
            sessionName = "OverviewApplicationList";

            try
            {
                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Append("select ApplicationId, ApplicationName, HDC, AWSUS, FDC, AWSEU,")
                        .Append("GrandTotal, ReportId")
                        .Append("from vwApplications")
                        .Append("where Active = 1");

                    result = dbFalcon.Fetch<Application>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<Application>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public List<FalconReport> GetEnvReports()
        {
            try
            {
                var query = PetaPoco.Sql.Builder
                    .Append("select distinct 'Environment|' + EnvApplication as ReportId, 'Falcon' as Module, 'Falcon' as Controller, ")
                    .Append("'ShowReport' as Action, EnvApplication as Name, 'Environment' as [Group], 'Report' as Type")
                    .Append("from Environment")
                    .Append("order by EnvApplication");

                return dbFalcon.Fetch<FalconReport>(query);
            }
            catch { throw; }
        }

        #region lookup details
        public List<DropDownItems> GetDropDownItems(string listName, string filter = null, bool exactMatch = false)
        {
            try
            {
                var query = PetaPoco.Sql.Builder;
                List<DropDownItems> client = new List<DropDownItems>();
                List<DropDownItems> servers = new List<DropDownItems>();

                if (listName == "clients")
                {
                    query = PetaPoco.Sql.Builder
                        .Append("select distinct clientname as ItemText, concat('c', client) as ItemValue")
                        .Append("from vwSiteInfoAll")
                        ;
                    if (!string.IsNullOrWhiteSpace(filter))
                    {
                        if (!exactMatch)
                            query.Append("where clientname like '%" + filter + "%'");
                        else
                            query.Append("where clientname = @0", filter);
                    }
                    //query.Append("union")

                    //query = PetaPoco.Sql.Builder
                    //    .Append("select url as ItemText, concat('u', site_id) as ItemValue")
                    //    .Append("from  vwSiteInfoAll")
                    //    .Append("where disabled = 0");

                    //if (!string.IsNullOrWhiteSpace(filter))
                    //    query.Append("and clientname like '%" + filter + "%'");

                    query.Append("order by 1");
                }
                else if (listName == "urls")
                {
                    query = PetaPoco.Sql.Builder
                        .Append("select distinct url as ItemText, site_id as ItemValue")
                        .Append("from vwSiteInfoAll")
                        ;
                    if (!string.IsNullOrWhiteSpace(filter))
                    {
                        if (!exactMatch)
                            query.Append("where url like '%" + filter + "%'");
                        else
                            query.Append("where url = @0", filter);
                    }
                    query.Append("order by url");
                }
                else if (listName == "servers")
                {
                    query = PetaPoco.Sql.Builder;

                    Regex rIP = new Regex(@"^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$");
                    Regex rURLFull = new Regex(@"^[a-zA-Z0-9\-\.]+\.(com|org|net|mil|edu|COM|ORG|NET|MIL|EDU)$");
                    Regex rURLStart = new Regex(@"^[a-zA-Z0-9\-\.]+\.$");
                    Regex rServer = new Regex(@"^(hdc|fra|i-)");

                    query.Append("select t1.ItemText, t1.ItemValue")
                        .Append("from")
                        .Append("(");

                    if (rIP.IsMatch(filter))
                    {
                        query.Append("select distinct Name as ItemText, concat('s', ComputerSystemID) as ItemValue")
                            .Append("from ComputerSystem cs")
                            .Append("inner join DeviceDiscovery dd on cs.DeviceDiscoveryID = dd.DeviceDiscoveryID")
                            .Append("inner join vwDNSMaster dm on dd.IPAddress = dm.IPAddress")
                            .Append("where dm.IPAddress = @0", filter);

                        query.Append("union")
                            .Append("select distinct dm.DNSName as ItemText,")
                            .Append("case substring(dm.DNSName, 1, 6) when 'hdcvcl' then concat('u', 0)")
                            .Append("else concat('u', SolutionInstanceID) end as ItemValue")
                            .Append("from SolutionInstance si")
                            .Append("inner join vwDNSMaster dm on si.PublicIPAddress = dm.IPAddress")
                            .Append("where dm.IPAddress = @0", filter);
                    }
                    else if (rURLFull.IsMatch(filter) || rURLStart.IsMatch(filter))
                    {
                        query.Append("select distinct url as ItemText, concat('u', site_id) as ItemValue")
                            .Append("from vwSiteInfoAll")
                            .Append("where url like '%" + filter + "%'");
                    }
                    else if (rServer.IsMatch(filter))
                    {
                        query.Append("select distinct s.ServerName as ItemText, concat('s', s.ServerID) as ItemValue")
                            .Append("from vwServerHardware s")
                            .Append("where s.ServerName like '%" + filter + "%'");

                        query.Append("union")
                            .Append("select distinct s.ServerName, concat('s', s.ServerID)")
                            .Append("from vwAWSInstance aws")
                            .Append("inner join vwServerHardware s")
                            .Append("on aws.ServerID = s.ServerID")
                            .Append("where aws.Active = 1")
                            .Append("where s.ServerName like '%" + filter + "%'");
                    }
                    else
                    {
                        query.Append("select distinct s.ServerName as ItemText, concat('s', s.ServerID) as ItemValue")
                            .Append("from vwServerHardware s");
                        if (!string.IsNullOrWhiteSpace(filter))
                            query.Append("where s.ServerName like '%" + filter + "%'");

                        query.Append("union")
                            .Append("select distinct s.ServerName, concat('s', s.ServerID)")
                            .Append("from vwAWSInstance aws")
                            .Append("inner join vwServerHardware s")
                            .Append("on aws.ServerID = s.ServerID")
                            .Append("where aws.Active = 1");
                        if (!string.IsNullOrWhiteSpace(filter))
                            query.Append("where s.ServerName like '%" + filter + "%'");

                        query.Append("union")
                            .Append("select distinct url as ItemText, concat('u', site_id) as ItemValue")
                            .Append("from vwSiteInfoAll")
                            ;
                        if (!string.IsNullOrWhiteSpace(filter))
                            query.Append("where url like '%" + filter + "%'");

                        query.Append("union")
                            .Append("select distinct clientname as ItemText, concat('c', client) as ItemValue")
                            .Append("from vwSiteInfoAll")
                            ;
                        if (!string.IsNullOrWhiteSpace(filter))
                            query.Append("where clientname like '%" + filter + "%'");

                        //query.Append("union")
                        //    .Append("select distinct url as ItemText, concat('u', site_id) as ItemValue")
                        //    .Append("from  vwSiteInfoAll")
                        //    ;
                        //if (!string.IsNullOrWhiteSpace(filter))
                        //    query.Append("where url like '%" + filter + "%'");

                        //query.Append("union")
                        //    .Append("select distinct Name as ItemText, concat('s', ComputerSystemID) as ItemValue")
                        //    .Append("from ComputerSystem cs")
                        //    .Append("inner join DeviceDiscovery dd on cs.DeviceDiscoveryID = dd.DeviceDiscoveryID")
                        //    .Append("inner join vwDNSMaster dm on dd.IPAddress = dm.IPAddress");
                        //if (!string.IsNullOrWhiteSpace(filter))
                        //    query.Append("where dm.IPAddress = @0", filter);

                        //query.Append("union")
                        //    .Append("select distinct dm.DNSName as ItemText,")
                        //    .Append("case substring(dm.DNSName, 1, 6) when 'hdcvcl' then concat('u', 0)")
                        //    .Append("else concat('u', SolutionInstanceID) end as ItemValue")
                        //    .Append("from SolutionInstance si")
                        //    .Append("inner join vwDNSMaster dm on si.PublicIPAddress = dm.IPAddress");
                        //if (!string.IsNullOrWhiteSpace(filter))
                        //    query.Append("where dm.IPAddress = @0", filter);

                        query.Append("union")
                            .Append("select dm.DNSName as ItemText, concat('u', si.SolutionInstanceID) as ItemValue")
                            .Append("from SolutionInstance si")
                            .Append("left join vwDNSMaster dm on si.PublicIPAddress = dm.IPAddress"); // and dm.DNSName not like 'hdcvcl%'");
                        if (!string.IsNullOrWhiteSpace(filter))
                        {
                            query.Append("where si.DBName = @0", filter)
                                .Append(" or si.RptDBName = @0", filter);
                        }

                        //query.Append("order by 2 desc, 1");
                    }

                    query.Append(") as t1")
                        .Append("order by substring(t1.ItemValue, 1, 1) desc, t1.ItemText");
                }

                var ddItems = dbFalcon.Fetch<DropDownItems>(query);

                return ddItems;
            }
            catch { throw; }
        }

        public List<RaveData> GetRaveUrlList(string reportName, string location = null, string env = null, string version = null, string db = null)
        {
            try
            {
                List<RaveData> result;
                sessionName = string.Format("{0}{1}{2}{3}{4}", reportName, location, env, version, db);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Select("site_id, clientname, url, RaveVersion, VersionName, dbname, sqlserver, urlloc, datacenter, team, urltype, dbsize, PageViews")
                        .From("vwSiteInfo");
                    //.Where("disabled = 0");
                    if (!string.IsNullOrWhiteSpace(location))
                        query.Append("where datacenter = @0", location);
                    if (!string.IsNullOrWhiteSpace(env))
                        query.Append("where urltype = @0", env);
                    if (!string.IsNullOrWhiteSpace(version))
                        query.Append("where RaveVersion = @0", version);
                    if (!string.IsNullOrWhiteSpace(db))
                        query.Append("where SQLServer = @0", db);
                    query.OrderBy("clientname ");

                    result = dbFalcon.Fetch<RaveData>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<RaveData>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public List<RaveURLHardware> GetRaveURLHardwareList(string reportName, string location = null, string env = null, string version = null, string db = null)
        {
            try
            {
                List<RaveURLHardware> result;
                sessionName = string.Format("{0}{1}{2}{3}{4}", reportName, location, env, version, db);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Append("select ClientName, URL, ServerName, HostName, VCenter, HWModel, Storage, RAID, DatastoreName, Environment")
                        .Append("from vwRaveURLHardware")
                        .Append("order by URL, ServerName");

                    result = dbFalcon.Fetch<RaveURLHardware>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<RaveURLHardware>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }
        public RaveData GetRaveSiteDetails(int id)
        {
            try
            {
                RaveData result;
                sessionName = string.Format("SiteDetails{0}", id);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Select("site_id, clientname, partner, datacenter, url, RaveVersion, team,")
                        .Append("envType, urltype, urlloc, SLA, SLApg, VIP, VPN, backendserver, backenddbname,")
                        .Append("sqlserver, dbname, dbsize, dbreplication, sqlserver_rpt, dbname_rpt,")
                        .Append("FTP, updatedby, updateddate, builtby, rel_date, req_pm, dr_priority,")
                        .Append("pd, csp, VersionName")
                        .From("vwSiteInfo")
                        .Where("site_id = @0", id);
                    //.Append("and disabled = 0");

                    result = dbFalcon.FirstOrDefault<RaveData>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (RaveData)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public List<SitePatch> GetRaveSitePatches(int id)
        {
            try
            {
                List<SitePatch> result;
                sessionName = string.Format("SitePatches{0}", id);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Select("id, site_id, PatchNumber, version, Description, RaveVersion, AppliedBy,")
                        .Append("AppServers, WebServers, DateApplied")
                        .From("vwGetSitePatches")
                        .Where("site_id = @0", id)
                        .OrderBy("DateApplied DESC, RaveVersion DESC"); //, PatchNumber DESC");

                    result = dbFalcon.Fetch<SitePatch>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<SitePatch>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public void GetRaveSitePatches(int id, out List<SitePatch> result, out string error)
        {
            PetaPoco.Database dbGetRaveSitePatches = new PetaPoco.Database("FalconDb");
            result = new List<SitePatch>();
            error = string.Empty;

            try
            {
                var query = PetaPoco.Sql.Builder
                    .Select("id, site_id, PatchNumber, version, Description, RaveVersion, AppliedBy,")
                    .Append("AppServers, WebServers, DateApplied")
                    .From("vwGetSitePatches")
                    .Where("site_id = @0", id)
                    .OrderBy("DateApplied DESC, RaveVersion DESC"); //, PatchNumber DESC");

                result = dbGetRaveSitePatches.Fetch<SitePatch>(query);
            }
            catch (Exception ex)
            {
                error = "GetRaveSitePatches: " + ex.Message;
            }
        }

        public List<SiteAddon> GetSiteAddons(int id)
        {
            try
            {
                List<SiteAddon> result;
                sessionName = string.Format("SiteAddons{0}", id);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Append("select distinct Name")
                        .Append("from vwAddons")
                        .Append("where site_id = @0", id);
                    //.OrderBy("name");

                    result = dbFalcon.Fetch<SiteAddon>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<SiteAddon>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public void GetSiteAddons(int id, out List<SiteAddon> result, out string error)
        {
            PetaPoco.Database dbGetSiteAddons = new PetaPoco.Database("FalconDb");
            result = new List<SiteAddon>();
            error = string.Empty;

            try
            {
                var query = PetaPoco.Sql.Builder
                    .Append("select distinct Name")
                    .Append("from vwAddons")
                    .Append("where site_id = @0", id);
                //.OrderBy("name");

                result = dbGetSiteAddons.Fetch<SiteAddon>(query);
            }
            catch (Exception ex)
            {
                error = "GetSiteAddons: " + ex.Message;
            }
        }

        public List<SiteAddon> GetServerAddons(string url, string serverName)
        {
            try
            {
                List<SiteAddon> result;
                sessionName = string.Format("ServerAddons{0}", serverName);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Append("select distinct Name")
                        .Append("from vwAddons")
                        .Append("where ServerName = @0", serverName);
                    //if(!string.IsNullOrWhiteSpace(url))
                    //    query.Append("and URL = @0", url);
                    //query.Append("order by Name");

                    result = dbFalcon.Fetch<SiteAddon>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<SiteAddon>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public List<ChildRowKVP> GetSiteAddOnDetails(string url, string addOnName)
        {
            try
            {
                //dbFalcon.OpenSharedConnection();
                //dbFalcon.OneTimeCommandTimeout = 30;

                List<ChildRowKVP> result;
                sessionName = string.Format("{0}{1}", url, addOnName);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Append("select distinct [Key], [Value]")
                        .Append("from vwAddOns")
                        .Append("where Url = @0", url)
                        .Append("and Name = @0", addOnName)
                        .Append("OPTION(RECOMPILE)");
                    //.Append("order by [Key]");

                    result = dbFalcon.Fetch<ChildRowKVP>(query);

                    // add environment name link
                    List<ChildRowKVP> envLinkList = GetEnvNameLinks(addOnName, url);
                    if (envLinkList.Count > 0)
                    {
                        // add to top of list
                        result.InsertRange(0, envLinkList);
                    }

                    // add safey gateway dianostic links
                    if (addOnName == "SafetyGateway")
                    {
                        List<ChildRowKVP> sgList = GetSGLinks(url);

                        if (envLinkList.Count > 0)
                            result.InsertRange(1, sgList);
                        else
                            result.InsertRange(0, sgList);
                    }

                    System.Web.HttpContext.Current.Session[sessionName] = result;

                    //dbFalcon.CloseSharedConnection();
                }
                else
                    result = (List<ChildRowKVP>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        private List<ChildRowKVP> GetEnvNameLinks(string addOnName, string url)
        {
            // if addon is BatchUploaderUI get the same server as BatchUploader
            if (addOnName == "BatchUploaderUI")
                addOnName = "BatchUploader";

            var envInfoQuery = PetaPoco.Sql.Builder
                .Append("select Id, EnvName, count(Client) as Total")
                .Append("from vwEnvironmentList")
                .Append("where EnvName in (")
                .Append("select distinct el.EnvName")
                .Append("from vwEnvironmentList el")
                .Append("inner join DNSMaster dm on el.ClientURL = dm.DNSName")
                .Append("inner join SolutionInstance si on dm.IPAddress = si.PublicIPAddress")
                .Append("where el.clienturl = @0", url)
                .Append("and el.EnvName like '" + addOnName + "%'")
                .Append(")")
                .Append("group by Id, EnvName");

            List<ReportingEnvTotal> envList = dbFalcon.Fetch<ReportingEnvTotal>(envInfoQuery);

            List<ChildRowKVP> envLinkList = new List<ChildRowKVP>();
            if (envList.Count > 0)
                envLinkList = BuildEnvURL(addOnName, envList);

            return envLinkList;
        }

        private List<ChildRowKVP> BuildEnvURL(string addOnName, List<ReportingEnvTotal> envList)
        {
            //<a href="/Falcon/Falcon/EnvironmentMapping/112?ClientCount=3&amp;reportId=AMReport22&amp;reportTitle=BOXI%20Environments">BOXI004</a>
            //
            List<ChildRowKVP> envLinkList = new List<ChildRowKVP>();
            string reportTitle = addOnName;
            var urlHost = System.Web.HttpContext.Current.Request.Url.Host;
            string link = "http://" + System.Web.HttpContext.Current.Request.Url.Host;
            if (System.Web.HttpContext.Current.Request.Url.Host == "localhost")
            {
                link = link + "/Falcon/Falcon/EnvironmentMapping/";
            }
            else
            {
                link = link + "/Falcon/EnvironmentMapping/";
            }

            foreach (ReportingEnvTotal envInfo in envList)
            {
                StringBuilder envLink = new StringBuilder()
                .Append(@"<a href=""")
                .Append(link)
                .Append(envInfo.Id)
                .Append(@"?ClientCount=")
                .Append(envInfo.Total)
                .Append(@"&reportId=Environment")
                .Append(@"&reportTitle=")
                .Append(reportTitle)
                .Append(@""">")
                .Append(envInfo.EnvName)
                .Append("</a>")
                ;

                ChildRowKVP kvp = new ChildRowKVP();
                kvp.Key = "EnvironmentName";
                kvp.Value = envLink.ToString();

                envLinkList.Add(kvp);
            }

            return envLinkList;
        }

        private List<ChildRowKVP> GetSGLinks(string url)
        {
            string segment = url.Substring(0, url.IndexOf('.'));
            string sgLink = BuildSGLink(string.Format(ConfigurationManager.AppSettings["SafetyGatewayDiagnosticLink"], segment));
            ChildRowKVP sg = new ChildRowKVP()
            {
                Key = "SafetyGatewayDiagnostic",
                Value = sgLink
            };
            string sgmLink = BuildSGLink(string.Format(ConfigurationManager.AppSettings["SafetyMappingDiagnosticLink"], segment));
            ChildRowKVP sgm = new ChildRowKVP()
            {
                Key = "SafetyMappingDiagnostic",
                Value = sgmLink
            };

            List<ChildRowKVP> sgList = new List<ChildRowKVP>();
            sgList.Add(sg);
            sgList.Add(sgm);

            return sgList;
        }

        private string BuildSGLink(string link)
        {
            StringBuilder sgLink = new StringBuilder()
            .Append(@"<a target=""_blank"" href=""")
            .Append(link)
            .Append(@""">")
            .Append(link)
            .Append("</a>")
            ;

            return sgLink.ToString();
        }

        public List<ChildRowKVP> GetServerAddOnDetails(string server, string addOnName, int? site_id = null)
        {
            try
            {
                List<ChildRowKVP> result;
                sessionName = string.Format("{0}{1}", server, addOnName);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Append("select distinct [Key], [Value]")
                        .Append("from vwAddOns")
                        .Append("where ServerName = @0", server)
                        .Append("and Name = @0", addOnName);
                    //if (site_id != null)
                    //    query.Append("and site_id = @0", site_id);
                    //query.Append("order by [Key]");

                    result = dbFalcon.Fetch<ChildRowKVP>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<ChildRowKVP>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public List<SiteInfo> GetRaveSiteClientInfo(int id)
        {
            try
            {
                List<SiteInfo> result;
                sessionName = string.Format("ClientInfo{0}", id);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Select("ColName, ColValue")
                        .From("vwClientInfo")
                        .Where("site_id = @0", id);

                    result = dbFalcon.Fetch<SiteInfo>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<SiteInfo>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public void GetRaveSiteClientInfo(int id, out List<SiteInfo> result, out string error)
        {
            PetaPoco.Database dbGetRaveSiteClientInfo = new PetaPoco.Database("FalconDb");
            result = new List<SiteInfo>();
            error = string.Empty;

            try
            {
                var query = PetaPoco.Sql.Builder
                    .Select("ColName, ColValue")
                    .From("vwClientInfo")
                    .Where("site_id = @0", id);

                result = dbGetRaveSiteClientInfo.Fetch<SiteInfo>(query);
            }
            catch (Exception ex)
            {
                error = "GetRaveSiteClientInfo: " + ex.Message;
            }
        }

        public List<SiteInfo> GetRaveSiteRaveInfo(int id)
        {
            try
            {
                List<SiteInfo> result;
                List<SiteInfo> result1;
                sessionName = string.Format("RaveInfo{0}", id);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Select("ColName, ColValue")
                        .From("vwRaveInfo")
                        .Where("site_id = @0", id);

                    //var query1 = PetaPoco.Sql.Builder
                    //    .Select("'TotalServers' as ColName, count(ServerName) as ColValue")
                    //    .From("vwURLServers")
                    //    .Where("site_id = @0", id);

                    var query2 = PetaPoco.Sql.Builder
                        .Select("ServerType + ' server count' as ColName, count(ServerName) as ColValue")
                        .From("vwURLServers")
                        .Where("site_id = @0", id)
                        .GroupBy("ServerType");



                    result = dbFalcon.Fetch<SiteInfo>(query);
                    //result1 = dbFalcon.Fetch<SiteInfo>(query1);
                    //result.AddRange(result1);
                    result1 = dbFalcon.Fetch<SiteInfo>(query2);
                    result.AddRange(result1);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<SiteInfo>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public void GetRaveSiteRaveInfo(int id, out List<SiteInfo> result, out string error)
        {
            PetaPoco.Database dbGetRaveSiteRaveInfo = new PetaPoco.Database("FalconDb");
            result = new List<SiteInfo>();
            error = string.Empty;

            try
            {
                List<SiteInfo> result1;

                var query = PetaPoco.Sql.Builder
                    .Select("ColName, ColValue")
                    .From("vwRaveInfo")
                    .Where("site_id = @0", id);
                result = dbGetRaveSiteRaveInfo.Fetch<SiteInfo>(query);

                var query2 = PetaPoco.Sql.Builder
                    .Select("ServerType + ' server count' as ColName, count(ServerName) as ColValue")
                    .From("vwURLServers")
                    .Where("site_id = @0", id)
                    .GroupBy("ServerType");
                result1 = dbGetRaveSiteRaveInfo.Fetch<SiteInfo>(query2);

                result.AddRange(result1);
            }
            catch (Exception ex)
            {
                error = "GetRaveSiteRaveInfo: " + ex.Message;
            }
        }

        public List<ServerListDetails> GetServerListDetails(int id)
        {
            try
            {
                var query = PetaPoco.Sql.Builder
                    .Append("select ServerId, ServerName, OSName, ClientName, URL, MemoryMB, MemoryGB, ProcessorCount, Model, ProcName, ServerType,")
                    .Append("PhysicalServerName, ClockSpeedMHz, ClockSpeedGHz, DataCenter, Location, SerialNumber")
                    .Append("from vwServerDetailsList")
                    .Append("where ServerID = @0", id);

                var serverInfo = dbFalcon.Fetch<ServerListDetails>(query);

                return serverInfo;
            }
            catch { throw; }
        }

        public string GetURL(int id)
        {
            try
            {
                string result;

                sessionName = string.Format("URL{0}", id);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Append("select dm.DNSName")
                        .Append("from SolutionInstance si")
                        .Append("inner join DNSMaster dm on si.PublicIPAddress = dm.IPAddress")
                        .Append("where si.SolutionInstanceID = @0", id)
                        .Append("and dm.DNSName not like 'hdcvcl%'");

                    result = dbFalcon.First<string>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (string)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public List<URLServer> GetURLServerList(int id)
        {
            try
            {
                List<URLServer> result;
                sessionName = string.Format("ServerList{0}", id);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Append("select site_id, ServerID, ServerType, ServerName, OSName, MemoryGB, ProcessorCount, URL")
                        .Append("from vwURLServers")
                        .Append("where site_id = @0", id)
                        .Append("order by ServerType, ServerName");

                    result = dbFalcon.Fetch<URLServer>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<URLServer>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public void GetURLServerList(int id, out List<URLServer> result, out string error)
        {
            PetaPoco.Database dbGetURLServerList = new PetaPoco.Database("FalconDb");
            result = new List<URLServer>();
            error = string.Empty;

            try
            {
                var query = PetaPoco.Sql.Builder
                    .Append("select site_id, ServerID, ServerType, ServerName, OSName, MemoryGB, ProcessorCount, URL")
                    .Append("from vwURLServers")
                    .Append("where site_id = @0", id)
                    .Append("order by ServerType, ServerName");

                result = dbGetURLServerList.Fetch<URLServer>(query);
            }
            catch (Exception ex)
            {
                error = "GetURLServerList: " + ex.Message;
            }
        }

        public List<ServerVolumeInfo> GetVolumeList(int id)
        {
            try
            {
                List<ServerVolumeInfo> result;
                sessionName = string.Format("VolumeList{0}", id);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Append("select ServerName, Path, VolumeLabel, CapacityMB, FreeSpaceMB, UsedSpaceMB,")
                        .Append("FileSytemType, ServerID, CapacityGB, FreeSpaceGB, UsedSpaceGB")
                        .Append("from vwServerVolumeInfo")
                        .Append("where ServerID = @0", id);

                    result = dbFalcon.Fetch<ServerVolumeInfo>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<ServerVolumeInfo>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public List<ServerNICInfo> GetNICList(int id)
        {
            try
            {
                List<ServerNICInfo> result;
                sessionName = string.Format("NICList{0}", id);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Append("select ServerID, NICID, IPAddress, MACAddress, ManufacturerName, Type")
                        .Append("from vwServerNICInfo")
                        .Append("where ServerID = @0", id)
                        //.Append("and IPAddressType = @0", "IPv4")
                        .Append("order by IPAddress");

                    result = dbFalcon.Fetch<ServerNICInfo>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<ServerNICInfo>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public List<ServerSoftware> GetServerSoftwareList(string serverName)
        {
            try
            {
                List<ServerSoftware> result;
                sessionName = string.Format("ServerSoftwareList{0}", serverName);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Select("DisplayName, DisplayVersion, InstallLocation, Publisher,")
                        .Append("Version, VersionMajor, VersionMinor, InstalledDT")
                        .From("vwServerSoftware")
                        .Where("ServerName = @0", serverName);

                    result = dbFalcon.Fetch<ServerSoftware>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<ServerSoftware>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public List<ServerService> GetServerServiceList(string serverName)
        {
            try
            {
                List<ServerService> result;
                sessionName = string.Format("ServerServiceList{0}", serverName);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Select("ProcessID, Name, DisplayName, Caption, Description,")
                        .Append("PathName, StartMode, Started, State, StartName")
                        .From("vwServerService")
                        .Where("ServerName = @0", serverName);

                    result = dbFalcon.Fetch<ServerService>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<ServerService>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public List<ReportingEnvTotal> GetEnvUrlList(string reportName, string env = null, string envUrl = null)
        {
            try
            {
                List<ReportingEnvTotal> result;
                //sessionName = string.Format("{0}{1}{2}", reportName, env, envUrl);

                //if (System.Web.HttpContext.Current.Session[sessionName] == null)
                //{
                var query = PetaPoco.Sql.Builder
                    .Select("EnvUrl, Client, ClientURL")
                    .From("vwEnvironmentList")
                    .Append("where EnvName like '" + env + "%'")
                    .Append("and EnvURL = @0", envUrl)
                    .OrderBy("Client ");

                result = dbFalcon.Fetch<ReportingEnvTotal>(query);

                //System.Web.HttpContext.Current.Session[sessionName] = result;
                //}
                //else
                //    result = (List<ReportingEnvTotal>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public List<ChildRowKVP> GetEnvDDL(string type, string param1 = null)
        {
            try
            {
                List<ChildRowKVP> result = new List<ChildRowKVP>();

                switch (type)
                {
                    case "envtype":
                        var query = PetaPoco.Sql.Builder
                            .Append("select EnvironmentType as [Key], EnvironmentType as [Value]")
                            .Append("from vwEnvironmentTypes")
                            .Append("order by EnvironmentType");

                        result = dbFalcon.Fetch<ChildRowKVP>(query);
                        break;

                    case "server":
                        query = PetaPoco.Sql.Builder
                            .Append("select s.ServerName as [Key], s.ServerName as [Value]")
                            .Append("from vwServerHardware s")
                            //.Append("union")
                            //.Append("select s.ServerName as [Key], s.ServerName as [Value]")
                            //.Append("from AWSInstance aws")
                            //.Append("inner join vwServerHardware s")
                            //.Append("on aws.ServerID = s.ServerID")
                            //.Append("where aws.Active = 1")
                            .Append("order by s.ServerName");

                        result = dbFalcon.Fetch<ChildRowKVP>(query);
                        break;

                    case "srvfunction":
                        query = PetaPoco.Sql.Builder
                            .Append("select distinct [Function] as [Key], [Function] as [Value]")
                            .Append("from vwServerFunction")
                            .Append("where [Function] is not null")
                            .Append("order by [Function]");

                        result = dbFalcon.Fetch<ChildRowKVP>(query);
                        break;

                    case "srvapplication":
                        ChildRowKVP kvp = new ChildRowKVP();
                        kvp.Key = "NA";
                        kvp.Value = "NA";
                        result.Add(kvp);
                        break;

                    case "url":
                        query = PetaPoco.Sql.Builder
                            .Select("url as [Key], url as [Value]")
                            .From("vwSiteInfo")
                            //.Where("disabled = 0")
                            .OrderBy("url");

                        result = dbFalcon.Fetch<ChildRowKVP>(query);
                        break;

                    case "urlrole":
                        kvp = new ChildRowKVP();
                        kvp.Key = "NA";
                        kvp.Value = "NA";
                        result.Add(kvp);
                        break;

                    case "dbserver":
                        query = PetaPoco.Sql.Builder
                            .Append("select distinct ServerName as [Key], ServerName as [Value]")
                            .Append("from vwDbServerDbName")
                            .Append("order by ServerName");

                        result = dbFalcon.Fetch<ChildRowKVP>(query);
                        break;

                    case "dbname":
                        if (param1 != null)
                        {
                            query = PetaPoco.Sql.Builder
                                .Select("DBName as [Key], DBName as [Value]")
                                .From("vwDbServerDbName")
                                .Where("ServerName = @0", param1)
                                .OrderBy("DBName");

                            result = dbFalcon.Fetch<ChildRowKVP>(query);
                        }
                        break;

                    case "dbrole":
                        kvp = new ChildRowKVP();
                        kvp.Key = "NA";
                        kvp.Value = "NA";
                        result.Add(kvp);
                        break;

                    case "envapplication":
                        query = PetaPoco.Sql.Builder
                            .Append("select distinct EnvApplication as [Key], EnvApplication as [Value]")
                            .Append("from vwEnvironmentList")
                            .Append("order by EnvApplication");

                        result = dbFalcon.Fetch<ChildRowKVP>(query);
                        break;
                }

                return result;
            }
            catch { throw; }
        }

        public List<RaveData> GetClientUrls(int id, string version = null)
        {
            try
            {
                List<RaveData> result;
                sessionName = string.Format("ClientUrls{0}{1}", id, version);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Select("site_id, clientname, url, RaveVersion, VersionName, dbname, sqlserver, urlloc, datacenter, team, urltype")
                        .From("vwSiteInfo")
                        .Where("client = @0", id);
                    if (!string.IsNullOrWhiteSpace(version))
                        query.Append("and RaveVersion = @0", version);

                    result = dbFalcon.Fetch<RaveData>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<RaveData>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public List<VMWareInfo> GetVMWareInfoGuest(string id)
        {
            try
            {
                List<VMWareInfo> result;
                sessionName = string.Format("VMWareInfoGuest{0}", id);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Select("DNSName, ColName, ColValue")
                        .From("vwVMInfoGuest")
                        .Where("DNSName like '" + id + "%'");

                    result = dbFalcon.Fetch<VMWareInfo>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<VMWareInfo>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public List<VMWareInfo> GetVMWareInfoHost(string id)
        {
            try
            {
                List<VMWareInfo> result;
                sessionName = string.Format("VMWareInfoHost{0}", id);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Select("Name, ColName, ColValue")
                        .From("vwVMInfoHost")
                        .Where("Name like '" + id + "%'");

                    result = dbFalcon.Fetch<VMWareInfo>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<VMWareInfo>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public List<CMPData> GetCMPData(string id)
        {
            try
            {
                List<CMPData> result;
                sessionName = string.Format("CMPData{0}", id);

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Append("select top 10 CMPNumber, WRNumber, CMPTitle, DateExecuted, ExecutedBy, Status ")
                        .Append("from CMPTracker.dbo.vwCMP")
                        .Append("where URL = @0", id)
                        .Append("order by CMPNumber desc");

                    result = dbCMPTracker.Fetch<CMPData>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<CMPData>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public void GetCMPData(string id, out List<CMPData> result, out string error)
        {
            result = new List<CMPData>();
            error = string.Empty;

            try
            {
                var query = PetaPoco.Sql.Builder
                    .Append("select top 10 CMPNumber, WRNumber, CMPTitle, DateExecuted, ExecutedBy, Status ")
                    .Append("from CMPTracker.dbo.vwCMP")
                    .Append("where URL = @0", id)
                    .Append("order by CMPNumber desc");

                result = dbCMPTracker.Fetch<CMPData>(query);
            }
            catch (Exception ex)
            {
                error = "GetCMPData: " + ex.Message;
            }
        }

        public List<DropDownItems> GetUrls()
        {
            try
            {
                List<DropDownItems> result;
                sessionName = "FalconGetUrls";

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var query = PetaPoco.Sql.Builder
                        .Select("url as ItemText, site_id as ItemValue")
                        .From("vwSiteInfo")
                        .OrderBy("url");

                    result = dbFalcon.Fetch<DropDownItems>(query);

                    System.Web.HttpContext.Current.Session[sessionName] = result;
                }
                else
                    result = (List<DropDownItems>)System.Web.HttpContext.Current.Session[sessionName];

                return result;
            }
            catch { throw; }
        }

        public NetworkInfo GetNetworkInfo(string url)
        {
            try
            {
                sessionName = string.Format("NetworkInfo{0}", url);
                NetworkInfo ni = new NetworkInfo();
                List<NetworkInfoAlias> aliases = new List<NetworkInfoAlias>();
                List<NetworkInfoPoolMember> poolMembers = new List<NetworkInfoPoolMember>();

                if (System.Web.HttpContext.Current.Session[sessionName] == null)
                {
                    var querySI = PetaPoco.Sql.Builder
                        .Select("VIP, VPN")
                        .From("vwSiteInfo")
                        .Where("url = @0", url);

                    var queryIP = PetaPoco.Sql.Builder
                        .Append("select ExtIPAddress, VirtualIP, PoolName, PoolStatus")
                        .Append("from fnGetPoolMembersByURL('" + url + "')");

                    var queryAL = PetaPoco.Sql.Builder
                        .Append("select distinct dns.DNSName, dns.IPAddress PublicIP, dns.VirtualIP, dns.IsCustomerFacing")
                        .Append("from DNSMaster dns")
                        .Append("inner join DNSMaster d2 on dns.IPAddress=d2.IPAddress")
                        .Append("where d2.DNSName = @0", url)
                        .Append("order by dns.IsCustomerFacing desc");

                    var queryPM = PetaPoco.Sql.Builder
                        .Select("PoolMember, PoolMemberIP")
                        .From("fnGetPoolMembersByURL('" + url + "')");

                    NetworkInfo niSI = dbFalcon.FirstOrDefault<NetworkInfo>(querySI);
                    NetworkInfo niIP = dbFalcon.FirstOrDefault<NetworkInfo>(queryIP);
                    aliases = dbFalcon.Fetch<NetworkInfoAlias>(queryAL);
                    poolMembers = dbFalcon.Fetch<NetworkInfoPoolMember>(queryPM);

                    ni.URL = url;
                    if (aliases.Count > 0)
                    {
                        ni.ExtIPAddress = aliases[0].PublicIP;
                        ni.VIP = aliases[0].VirtualIP != null ? aliases[0].VirtualIP : niIP.VirtualIP;
                    }
                    if (ni.VPN != null)
                        ni.VPN = niSI.VPN;
                    if (niIP != null)
                    {
                        ni.VIP = niIP.VirtualIP;
                        ni.PoolName = niIP.PoolName;
                        ni.PoolStatus = niIP.PoolStatus;
                    }
                    ni.Aliases = aliases;
                    ni.PoolMembers = poolMembers;

                    System.Web.HttpContext.Current.Session[sessionName] = ni;
                }
                else
                    ni = (NetworkInfo)System.Web.HttpContext.Current.Session[sessionName];

                return ni;
            }
            catch { throw; }
        }

        public void GetNetworkInfo(string url, out NetworkInfo ni, out string error)
        {
            PetaPoco.Database dbGetNetworkInfo1 = new PetaPoco.Database("FalconDb");
            PetaPoco.Database dbGetNetworkInfo2 = new PetaPoco.Database("FalconDb");
            ni = new NetworkInfo();
            error = string.Empty;

            try
            {
                List<NetworkInfoAlias> aliases = new List<NetworkInfoAlias>();
                List<NetworkInfoPoolMember> poolMembers = new List<NetworkInfoPoolMember>();

                var querySI = PetaPoco.Sql.Builder
                    .Select("VIP, VPN")
                    .From("vwSiteInfo")
                    .Where("url = @0", url);

                var queryIP = PetaPoco.Sql.Builder
                    .Append("select ExtIPAddress, VirtualIP, PoolName, PoolStatus")
                    .Append("from fnGetPoolMembersByURL('" + url + "')");

                var queryAL = PetaPoco.Sql.Builder
                    .Append("select dns.DNSName, dns.IPAddress PublicIP, dns.VirtualIP, dns.IsCustomerFacing")
                    .Append("from DNSMaster dns")
                    .Append("inner join DNSMaster d2 on dns.IPAddress=d2.IPAddress")
                    .Append("where d2.DNSName = @0", url)
                    .Append("order by dns.IsCustomerFacing desc");

                var queryPM = PetaPoco.Sql.Builder
                    .Select("PoolMember, PoolMemberIP")
                    .From("fnGetPoolMembersByURL('" + url + "')");

                NetworkInfo niSI = dbGetNetworkInfo1.FirstOrDefault<NetworkInfo>(querySI);
                NetworkInfo niIP = dbGetNetworkInfo2.FirstOrDefault<NetworkInfo>(queryIP);
                aliases = dbFalcon.Fetch<NetworkInfoAlias>(queryAL);
                poolMembers = dbFalcon.Fetch<NetworkInfoPoolMember>(queryPM);

                ni.URL = url;
                if (aliases.Count > 0)
                {
                    ni.ExtIPAddress = aliases[0].PublicIP;
                    ni.VIP = aliases[0].VirtualIP != null ? aliases[0].VirtualIP : niIP.VirtualIP;
                }
                if (ni.VPN != null)
                    ni.VPN = niSI.VPN;
                if (niIP != null)
                {
                    ni.VIP = niIP.VirtualIP;
                    ni.PoolName = niIP.PoolName;
                    ni.PoolStatus = niIP.PoolStatus;
                }
                ni.Aliases = aliases;
                ni.PoolMembers = poolMembers;
            }
            catch (Exception ex)
            {
                error = "GetNetworkInfo:" + ex.Message;
            }
        }

        public List<WebServerInfomation> GetWebServerInfo(int id)
        {
            try
            {
                var query = PetaPoco.Sql.Builder
                    .Append("select SolutionInstanceID, URL, ServerName, Config, RemotingServer, [SSL], CleaningEngine, SessionState")
                    .Append("from vwWebServerInformation")
                    .Append("where SolutionInstanceID = @0", id);

                return dbFalcon.Fetch<WebServerInfomation>(query);
            }
            catch { throw; }
        }

        public void GetWebServerInfo(int id, out List<WebServerInfomation> result, out string error)
        {
            PetaPoco.Database dbGetWebServerInfo = new PetaPoco.Database("FalconDb");
            result = new List<WebServerInfomation>();
            error = string.Empty;

            try
            {
                var query = PetaPoco.Sql.Builder
                    .Append("select SolutionInstanceID, URL, ServerName, Config, RemotingServer, [SSL], isnull(CleaningEngine, 'false') as CleaningEngine, SessionState")
                    .Append("from vwWebServerInformation")
                    .Append("where SolutionInstanceID = @0", id);

                result = dbGetWebServerInfo.Fetch<WebServerInfomation>(query);
            }
            catch (Exception ex)
            {
                error = "GetWebServerInfo: " + ex.Message;
            }
        }

        public List<AppServerInfomation> GetAppServerInfo(int id)
        {
            try
            {
                var query = PetaPoco.Sql.Builder
                    .Append("select SolutionInstanceID, URL, ServerName, Config, ServiceName, RemotingServer, isnull(PDF, 'false') as PDF, ")
                    .Append("isnull(DeferredStatusRollups, 'false') as DeferredStatusRollups, isnull(ClinicalViews, 'false') as ClinicalViews, ")
                    .Append("isnull(RemotingServer, 'false') as RemotingServer, MaxMigrationThreads, isnull(MigrationKey, 'false') as MigrationKey, PDFBuffer, PDFLocation")
                    .Append("from vwAppServerInformation")
                    .Append("where SolutionInstanceID = @0", id);

                return dbFalcon.Fetch<AppServerInfomation>(query);
            }
            catch { throw; }
        }

        public void GetAppServerInfo(int id, out List<AppServerInfomation> result, out string error)
        {
            PetaPoco.Database dbGetAppServerInfo = new PetaPoco.Database("FalconDb");
            result = new List<AppServerInfomation>();
            error = string.Empty;

            try
            {
                var query = PetaPoco.Sql.Builder
                    .Append("select SolutionInstanceID, URL, ServerName, Config, ServiceName, RemotingServer, PDF, DeferredStatusRollups, ClinicalViews,")
                    .Append("RemotingServer, MaxMigrationThreads, MigrationKey, PDFBuffer, PDFLocation")
                    .Append("from vwAppServerInformation")
                    .Append("where SolutionInstanceID = @0", id);

                result = dbGetAppServerInfo.Fetch<AppServerInfomation>(query);
            }
            catch (Exception ex)
            {
                error = "GetAppServerInfo: " + ex.Message;
            }
        }

        public List<RaveViewerInfomation> GetRaveViewerInfo(int id)
        {
            try
            {
                var query = PetaPoco.Sql.Builder
                    .Append("select distinct SolutionInstanceID, URL, ServerName, ViewerURL, UniqueName, Location")
                    .Append("from vwRaveViewerInformation")
                    .Append("where SolutionInstanceID = @0", id);

                return dbFalcon.Fetch<RaveViewerInfomation>(query);
            }
            catch { throw; }
        }

        public void GetRaveViewerInfo(int id, out List<RaveViewerInfomation> result, out string error)
        {
            PetaPoco.Database dbGetRaveViewerInfo = new PetaPoco.Database("FalconDb");
            result = new List<RaveViewerInfomation>();
            error = string.Empty;

            try
            {
                var query = PetaPoco.Sql.Builder
                    .Append("select distinct SolutionInstanceID, URL, ServerName, ViewerURL, UniqueName, Location")
                    .Append("from vwRaveViewerInformation")
                    .Append("where SolutionInstanceID = @0", id);

                result = dbGetRaveViewerInfo.Fetch<RaveViewerInfomation>(query);
            }
            catch (Exception ex)
            {
                error = ex.Message;
            }
        }

        #endregion

        public string HeathCheck()
        {

            string status = "Green";

            try
            {
                int ret = dbFalcon.Execute("select top 1 * from SolutionInstance");

                if (ret == 0)
                    status = "Red";
            }
            catch
            {
                status = "Red";
                throw;
            }

            return status;
        }

        public List<ScriptLastRunDate> GetLastRunDate()
        {
            try
            {
                List<ScriptLastRunDate> reportData = null;

                reportData = dbFalcon.Fetch<ScriptLastRunDate>("select * from vwScriptLastRunDate");

                return reportData;
            }
            catch { throw; }
        }
    }

}