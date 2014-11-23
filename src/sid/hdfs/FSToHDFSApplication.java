/**
 * This copy the files from the File System to HDFS using Hadoop Java API
 * This uses hadoop-0.23.3-dev-core.jar
 * Tested on hadoop-2.4.0.
 */
package sid.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author sidnan
 *
 */
public class FSToHDFSApplication {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		try{
			Path pt=new Path("hdfs://localhost:9000/user/sidnan/input/MyDataFolder");
			FileSystem hdfs = FileSystem.get(new Configuration());
			
            if(!hdfs.exists(pt))
            {
            	hdfs.mkdirs(pt);     //Create new Directory
                System.out.println("Folder Created.");
            }
            //Copying File from local to HDFS

            Path localFilePath = new Path("/home/sidnan/workspace/FSToHDFS/WordList");
            hdfs.copyFromLocalFile(localFilePath, pt);
            System.out.println("File copied from local to HDFS.");
		}catch(Exception e){
		}
	}

}
