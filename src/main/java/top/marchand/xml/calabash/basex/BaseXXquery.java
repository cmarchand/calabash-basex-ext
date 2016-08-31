package top.marchand.xml.calabash.basex;

import com.xmlcalabash.library.DefaultStep;
import com.xmlcalabash.core.XMLCalabash;
import com.xmlcalabash.io.WritablePipe;
import com.xmlcalabash.core.XProcRuntime;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.XdmSequenceIterator;

import com.xmlcalabash.runtime.XAtomicStep;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import javax.xml.transform.stream.StreamSource;
import net.sf.saxon.s9api.Axis;
import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import org.basex.examples.api.BaseXClient;

@XMLCalabash(
        name = "ex:basex-xquery",
        type = "{http://marchand.top/xmlcalabash/steps}basex-xquery")

/**
 * This step executes a query on a basex server.
 * <ul>
 * <li>input port <b><tt>xquery</tt></b> contains the xquery to execute</li>
 * <li>output port <b><tt>result</tt></b> contains the result sequence</li>
 * </ul>
 * TODO: add a step to give the connect XML informations. XML structure should be
 * the same as {@link https://github.com/cmarchand/xpath-basex-ext/blob/master/src/main/java/fr/efl/saxon/basex/BaseXQuery.java}
 */
public class BaseXXquery extends DefaultStep {

    public static final transient String XQUERY_INPUT_PORT_NAME = "xquery";
    public static final transient String XQUERY_OUTPUT_PORT = "result";
    // TODO: get those informations from a port of the step
    private static final String CONNECT_STRING =
            "<?xml version='1.0' encoding='UTF-8'?>"+
            "<basex>"+
                "<server>localhost</server>"+
                "<port>1984</port>"+
                "<user>admin</user>"+
                "<password>admin</password>"+
            "</basex>";
   
    public BaseXXquery(XProcRuntime runtime, XAtomicStep step) {
        super(runtime,step);
    }

    @Override
    public void run() throws SaxonApiException {
    	
        Processor proc = runtime.getProcessor();
        // first, parse connect string
        DocumentBuilder builder = proc.newDocumentBuilder();
        XdmNode basexNode = builder.build(new StreamSource(new ByteArrayInputStream(CONNECT_STRING.getBytes(Charset.forName("UTF-8")))));
        String server=null, port=null, user=null, password=null;
        XdmSequenceIterator iterator=basexNode.axisIterator(Axis.CHILD);
        for(XdmItem ni = iterator.next(); ni!=null; iterator.hasNext()) {
            if(ni instanceof XdmNode) {
                XdmNode no = (XdmNode)ni;
                switch (no.getNodeName().getLocalName()) {
                    case "server":
                        server = ni.getStringValue();
                        break;
                    case "port":
                        port = ni.getStringValue();
                        break;
                    case "user":
                        user = ni.getStringValue();
                        break;
                    case "password":
                        password = ni.getStringValue();
                        break;
                    default:
                        throw new SaxonApiException("child elements of basex must be server, port, user and password");
                }
            }
        }
        // TODO : previous code must be changed to get the connect informations from an input port
        String xquery = step.getInput(XQUERY_INPUT_PORT_NAME).toString();
        // TODO: check that xquery is not null !
        

        try {
            BaseXClient session = new BaseXClient(server, Integer.parseInt(port), user, password);
            BaseXClient.Query query = session.query(xquery);
            WritablePipe outputer = step.getOutput(XQUERY_OUTPUT_PORT).getWriter();
            while(query.more()) {
                StreamSource source = new StreamSource(new ByteArrayInputStream(query.next().getBytes(BaseXClient.UTF8)));
                XdmNode node = builder.build(source);
                outputer.write(node);
            }
        } catch(IOException ex) {
            throw new SaxonApiException(ex);
        }
    }   
    
}
