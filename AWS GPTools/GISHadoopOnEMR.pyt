import arcpy
import boto
import time
import os
import stat

from boto.emr import *
from boto.emr.connection import *
from boto.emr.step import *
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from time import sleep

class Toolbox(object):
    def __init__(self):

        self.label = "Toolbox"
        self.alias = ""

        # List of tool classes associated with this toolbox
        self.tools = [EMRSetup, EMRStatus, EMRTerminate, S3Upload, S3Download, RunHiveQuery]


class EMRSetup(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "EMR Setup"
        self.description = ""
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""

        # First parameter
        param0 = arcpy.Parameter(
            displayName="Job Name",
            name="jname",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param0.value = "GIS_Tools_for_Hadoop_in_ArcMap"

        # Second parameter
        param1 = arcpy.Parameter(
            displayName="AWS Access Key ID",
            name="awskey",
            datatype="String",
            parameterType="Required",
            direction="Input")


        param1.value = ""


        # Third parameter
        param2 = arcpy.Parameter(
            displayName="AWS Secret Access Key",
            name="awssecretkey",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param2.value = ""



        # Fourth parameter
        param3 = arcpy.Parameter(
            displayName="AWS Region",
            name="region",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param3.value = ""

        # Fifth parameter
        param4 = arcpy.Parameter(
            displayName="EC2 Key Pair Name",
            name="ec2keyname",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param4.value = ""

        # Sixth parameter
        param5 = arcpy.Parameter(
            displayName="EC2 Machine Size",
            name="machinesize",
            datatype="String",
            parameterType="Optional",
            direction="Input")

        param5.value = "m1.xlarge"

        # Seventh parameter
        param6 = arcpy.Parameter(
            displayName="EMR Cluster Number of Machines",
            name="clustersize",
            datatype="String",
            parameterType="Optional",
            direction="Input")

        param6.value = "4"

        # Eigth parameter
        param7 = arcpy.Parameter(
            displayName="S3 Log File URI",
            name="loguri",
            datatype="String",
            parameterType="Optional",
            direction="Input")



        # Ninth parameter
        param8 = arcpy.Parameter(
            displayName="Job Flow ID",
            name="jobid",
            datatype="String",
            parameterType="Derived",
            direction="Output")


       # Tenth parameter
        param9 = arcpy.Parameter(
            displayName="EMR Master DNS Name",
            name="dns",
            datatype="String",
            parameterType="Derived",
            direction="Output")


        params = [param0, param1, param2, param3, param4, param5, param6, param7, param8, param9]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""

        jname = parameters[0].value
        awskey = parameters[1].value
        awssecretkey = parameters[2].value
        region = parameters[3].value
        ec2keyname = parameters[4].value
        machinesize = parameters[5].value
        clustersize = parameters[6].value
        loguri = parameters[7].value


        if machinesize == None :
            machinesize = "m1.xlarge"

        if clustersize == None :
            clustersize = 4

        bootstrap= BootstrapAction(name='custom', path='s3://marwa.gishadoop.com/download.sh', bootstrap_action_args='')
        arcpy.AddMessage("Bootstraping GIS Tools for Hadoop on EMR")
        arcpy.AddMessage("Connecting to AWS %s" % region )
        conn = connect_to_region(region, aws_access_key_id= awskey , aws_secret_access_key= awssecretkey)
        jobid = conn.run_jobflow(name = jname,ami_version='latest',enable_debugging=True,log_uri=loguri,keep_alive=True,ec2_keyname= ec2keyname, master_instance_type = machinesize, slave_instance_type= machinesize,num_instances=clustersize , bootstrap_actions=[bootstrap] , steps=[InstallHiveStep()])


        status = conn.describe_jobflow(jobid)
        parameters[8].value=status.jobflowid
        arcpy.AddMessage("New Job Flow ID: %s" % status.jobflowid)
        #arcpy.AddMessage(type(status))
        while True:
            status = conn.describe_jobflow(jobid)
            arcpy.AddMessage("Status: {}".format(status.state))
            if status.state == 'WAITING':
                arcpy.AddMessage("Ready to use!")
                parameters[9].value= status.masterpublicdnsname
                arcpy.AddMessage("DNS name to access master node is: {}".format(status.masterpublicdnsname))
                break
       	    sleep(30)

        return




class EMRStatus(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "EMR Status"
        self.description = ""
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""

        # First parameter
        param0 = arcpy.Parameter(
            displayName="Job Flow ID",
            name="jobid",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param0.value = "j-************"

        # Second parameter
        param1 = arcpy.Parameter(
            displayName="AWS Access Key ID",
            name="awskey",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param1.value = ""


        # Third parameter
        param2 = arcpy.Parameter(
            displayName="AWS Secret Access Key",
            name="awssecretkey",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param2.value = ""

        # Fourth parameter
        param3 = arcpy.Parameter(
            displayName="AWS Region",
            name="region",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param3.value = ""


        # Fifth parameter
        param4 = arcpy.Parameter(
            displayName="Job Flow ID Status",
            name="status",
            datatype="String",
            parameterType="Derived",
            direction="Output")

        params = [param0, param1, param2, param3, param4]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""

        jobid = parameters[0].value
        awskey = parameters[1].value
        awssecretkey = parameters[2].value
        region = parameters[3].value

        conn = connect_to_region(region, aws_access_key_id= awskey , aws_secret_access_key= awssecretkey)
        status = conn.describe_jobflow(jobid)

        arcpy.AddMessage("Cluster Status is: {}".format(status.state))
        parameters[4].value = status.state

        return


class EMRTerminate(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "EMR Terminate"
        self.description = ""
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""

        # First parameter
        param0 = arcpy.Parameter(
            displayName="Job Flow ID",
            name="jobid",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param0.value = "j-************"

        # Second parameter
        param1 = arcpy.Parameter(
            displayName="AWS Access Key ID",
            name="awskey",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param1.value = ""


        # Third parameter
        param2 = arcpy.Parameter(
            displayName="AWS Secret Access Key",
            name="awssecretkey",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param2.value = ""

        # Fourth parameter
        param3 = arcpy.Parameter(
            displayName="AWS Region",
            name="region",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param3.value = ""


        # Fifth parameter
        param4 = arcpy.Parameter(
            displayName="Job Flow ID Status",
            name="status",
            datatype="String",
            parameterType="Derived",
            direction="Output")


        params = [param0, param1, param2, param3, param4]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""

        jobid = parameters[0].value
        awskey = parameters[1].value
        awssecretkey = parameters[2].value
        region = parameters[3].value


        conn = connect_to_region(region, aws_access_key_id= awskey , aws_secret_access_key= awssecretkey)
        status = conn.describe_jobflow(jobid)

        if status.state != 'TERMINATED':
            arcpy.AddMessage("Terminating EMR Cluster...")
            conn.terminate_jobflow(jobid)
            sleep(20)
        arcpy.AddMessage("Cluster Status is: {}".format(status.state))
        parameters[4].value=status.state
        return


class S3Upload(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "S3 Upload"
        self.description = "Upload a file from a local folder to Amazon Web Services S3"
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""

        # First parameter
        param0 = arcpy.Parameter(
            displayName="AWS Access Key ID",
            name="awskey",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param0.value = ""

        # Second parameter
        param1 = arcpy.Parameter(
            displayName="AWS Secret Access Key",
            name="awssecretkey",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param1.value = ""

         # Third parameter
        param2 = arcpy.Parameter(
            displayName="Bucket Name",
            name="bucketname",
            datatype="GPString",
            parameterType="Required",
            direction="Input")

        # Fourth parameter
        param3 = arcpy.Parameter(
            displayName="Key Names",
            name="subfolder",
            datatype="GPString",
            parameterType="Optional",
            direction="Input")

        # Fifth parameter
        param4 = arcpy.Parameter(
            displayName="File to upload",
            name="fn",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")


        params = [param0, param1, param2, param3, param4]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""

        if (parameters[0].altered and parameters[1].altered) or (parameters[0].value is not None and parameters[1].value is not None):
            awskey = parameters[0].value
            awssecretkey = parameters[1].value
            conn = S3Connection(awskey, awssecretkey)
            rs = conn.get_all_buckets()
            bucket_list = []
            for x in range (0, (len(rs))):
                bucket_list.append(  rs[x].name.encode(encoding='UTF-8') )
            parameters[2].filter.list = bucket_list

            if parameters[2].value != None:
                mybucket = conn.get_bucket(parameters[2].value)
                fs = mybucket.list().bucket.get_all_keys()
                file_list = []
                for y in range (0, (len(fs))):
                    file_list.append(  fs[y].name.encode(encoding='UTF-8') )
                parameters[3].filter.list = file_list

        if not parameters[2].value == None and not parameters[2].value in bucket_list:
                bucket_list.append(parameters[2].value)
                parameters[2].filter.list = bucket_list

        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""

        awskey = parameters[0].value
        awssecretkey = parameters[1].value
        bucketname = parameters[2].value
        keyname = parameters[3].value
        fn = parameters[4].valueAsText


        def cb_progress(complete, total):
            if complete == 0:
                arcpy.SetProgressor("step", "Uploading {0} bytes to S3" .format(total), 0, total, 2)
            if complete != total:
                arcpy.SetProgressorLabel("Uploaded {0} bytes from {1} bytes".format(complete, total))
                arcpy.SetProgressorPosition(complete % total )
                time.sleep(2)


        conn = S3Connection(awskey, awssecretkey)
        bucketcheck = conn.lookup(bucketname)
        if bucketcheck is None:
            bucket = conn.create_bucket(bucketname)
            arcpy.AddMessage("Bucket name did not exist, creating new bucket...")
        else:
            bucket = conn.get_bucket(bucketname)

        arcpy.AddMessage("Start file upload: {}".format(fn))
        if keyname is None:
            k = Key(bucket)
            k.key = os.path.basename(fn)

        else:
            file_name = os.path.basename(fn)
            full_key_name = os.path.join(keyname, file_name)
            k = bucket.new_key(full_key_name)

        k.set_contents_from_filename(fn, cb=cb_progress, num_cb=100 )
        k.set_acl('private')
        arcpy.AddMessage("File upload successful!")

        return



class S3Download(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "S3 Download"
        self.description = "Download a file from Amazon Web Services S3 to local folder"
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""

        # First parameter
        param0 = arcpy.Parameter(
            displayName="AWS Access Key ID",
            name="awskey",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param0.value = ""

        # Second parameter
        param1 = arcpy.Parameter(
            displayName="AWS Secret Access Key",
            name="awssecretkey",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param1.value = ""

         # Third parameter
        param2 = arcpy.Parameter(
            displayName="Bucket Name",
            name="bucketname",
            datatype="GPString",
            parameterType="Required",
            direction="Input")

        # Fourth parameter
        param3 = arcpy.Parameter(
            displayName="File to download",
            name="filename",
            datatype="GPString",
            parameterType="Required",
            direction="Input")

        # Fifth parameter
        param4 = arcpy.Parameter(
            displayName="Local file path",
            name="filepath",
            datatype="DEFolder",
            parameterType="Required",
            direction="Input")

        params = [param0, param1, param2, param3, param4]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""

        if (parameters[0].altered and parameters[1].altered) or (parameters[0].value is not None and parameters[1].value is not None):
            awskey = parameters[0].value
            awssecretkey = parameters[1].value
            conn = S3Connection(awskey, awssecretkey)
            rs = conn.get_all_buckets()
            bucket_list = []
            for x in range (0, (len(rs))):
                bucket_list.append(  rs[x].name.encode(encoding='UTF-8') )
            parameters[2].filter.list = bucket_list

            if parameters[2].value != None:
                mybucket = conn.get_bucket(parameters[2].value)
                fs = mybucket.list().bucket.get_all_keys()
                file_list = []
                for y in range (0, (len(fs))):
                    file_list.append(  fs[y].name.encode(encoding='UTF-8') )
                parameters[3].filter.list = file_list


        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""


        awskey = parameters[0].value
        awssecretkey = parameters[1].value
        bucketname = parameters[2].value
        filename = parameters[3].value
        localfolder = parameters[4].value


        def cb_progress(complete, total):
            if complete == 0:
                arcpy.SetProgressor("step", "Downloading {0} bytes from S3" .format(total), 0, total, 2)
            if complete != total:
                arcpy.SetProgressorLabel("Downloaded {0} bytes from {1} bytes".format(complete, total))
                arcpy.SetProgressorPosition(complete % total )
                time.sleep(2)

        conn = S3Connection(awskey, awssecretkey)
        bucketcheck = conn.lookup(bucketname)
        if bucketcheck is not None:
            bucket = conn.get_bucket(bucketname)

        k = Key(bucket)
        k.key = filename

        if not os.path.isfile(filename):
            thefile = filename + ".txt"

        locpath = os.path.join(str(localfolder), (str(thefile)).replace("/", "_"))
        k.get_contents_to_filename(locpath ,cb=cb_progress, num_cb=100 )

        arcpy.AddMessage("File download successful!")

        return










class RunHiveQuery(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Run Hive Query"
        self.description = ""
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""

        # First parameter
        param0 = arcpy.Parameter(
            displayName="Job Flow ID",
            name="jobid",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param0.value = "j-************"

        # Second parameter
        param1 = arcpy.Parameter(
            displayName="AWS Access Key ID",
            name="awskey",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param1.value = ""


        # Third parameter
        param2 = arcpy.Parameter(
            displayName="AWS Secret Access Key",
            name="awssecretkey",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param2.value = ""

        # Fourth parameter
        param3 = arcpy.Parameter(
            displayName="AWS Region",
            name="region",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param3.value = ""

       # Fifth parameter
        param4 = arcpy.Parameter(
            displayName="S3 Path for Hive Query File",
            name="hqf",
            datatype="String",
            parameterType="Required",
            direction="Input")

        param4.value = ""

        # Sixth parameter
        param5 = arcpy.Parameter(
            displayName="S3 Query Result Path",
            name="qresult",
            datatype="String",
            parameterType="Optional",
            direction="Input")

        param5.value = ""

        params = [param0, param1, param2, param3, param4, param5]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""

        jobid = parameters[0].value
        awskey = parameters[1].value
        awssecretkey = parameters[2].value
        region = parameters[3].value
        s3_input_path = parameters[4].value
        s3_output_path= parameters[5].value

        conn = connect_to_region(region, aws_access_key_id=awskey, aws_secret_access_key=awssecretkey)

        steps=[
        ScriptRunnerStep(name='Copy Hive Script', step_args=['s3://tobinbak-test/bootstrap-actions/copy-s3-to-local',
            '-s', s3_input_path, '-d', "/home/hadoop/hive-emr-commands.hql"]),
        HiveStep('Hive Sample', "/home/hadoop/hive-emr-commands.hql", hive_args=['-d', "OUTPUT=%s" % s3_output_path])]

        response = conn.add_jobflow_steps(jobid, steps)

        while True:
            sleep(30)
            status = response.connection.describe_jobflow(jobid)
            if status.state == 'WAITING':
                arcpy.AddMessage("Query completed, results are now available in the output location!")
                break



        return






##def main():
##    tbx = Toolbox()
##    tool = EMRSetup()
##    tool.execute(tool.getParameterInfo(), None)
##
##if __name__ == '__main__':
##    main()