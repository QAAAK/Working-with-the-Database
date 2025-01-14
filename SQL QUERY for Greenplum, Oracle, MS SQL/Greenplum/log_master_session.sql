select logtime, loguser, logdatabase, logpid, logsession, logsessiontime, logmessage, logdetail 
from gp_toolkit."__gp_log_master_ext"
order by logsessiontime desc
