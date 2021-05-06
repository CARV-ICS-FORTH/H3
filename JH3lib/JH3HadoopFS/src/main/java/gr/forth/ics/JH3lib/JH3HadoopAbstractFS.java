package gr.forth.ics.JH3lib;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.DelegateToFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * H3 implementation of AbstractFileSystem.
 * This implementation delegates to the H3FileSystem
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class JH3HadoopAbstractFS extends DelegateToFileSystem {

	public JH3HadoopAbstractFS(URI theUri, Configuration conf) throws IOException, URISyntaxException {
		super(theUri, new JH3HadoopFS(), conf, "h3", false);
	}
   
	@Override
	public int getUriDefaultPort(){
		// TODO Change to some predefined constant(e.g. Constants.H3_DEFAULT_PORT)
		return 8000;
	}
}
