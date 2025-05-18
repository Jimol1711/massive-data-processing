package cl.uchile.pmd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.AlreadyBoundException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

/**
 * Main method to search articles using Elasticsearch.
 * 
 * @author Aidan, Alberto
 */
public class SearchWikiIndex {
	
	// used to keep track of the names of fields
	// that we are using
	public enum FieldNames {
		URL, TITLE, MODIFIED, ABSTRACT, RANK
	}

	// used to assign higher/lower ranking
	// weight to different document fields
	public static final HashMap<String, Float> BOOSTS = new HashMap<String, Float>();
	static {
		BOOSTS.put(FieldNames.ABSTRACT.name(), 1f); // <- default
		BOOSTS.put(FieldNames.TITLE.name(), 5f);
	}

	public static final int DOCS_PER_PAGE = 10;

	public static void main(String args[]) throws IOException, ClassNotFoundException, AlreadyBoundException,
			InstantiationException, IllegalAccessException {
		Option inO = new Option("i", "input elasticsearch index name");
		inO.setArgs(1);
		inO.setRequired(true);

		Options options = new Options();
		options.addOption(inO);

		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println("***ERROR: " + e.getClass() + ": " + e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options);
			return;
		}

		// print help options and return
		if (cmd.hasOption("h")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options);
			return;
		}

		TransportClient client = ElasticsearchCluster.getTransportClient();
		
		String indexName = cmd.getOptionValue(inO.getOpt());
		System.err.println("Querying index at  " + indexName);

		startSearchApp(client,indexName);
		
		client.close();
	}

	/**
	 * 
	 * @param inDirectory : the location of the index directory
	 * @throws IOException
	 */
	public static void startSearchApp(TransportClient client, String indexName) throws IOException {
		// we open a UTF-8 reader over std-in
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in, "utf-8"));

		while (true) {
			System.out.println("Enter a keyword search phrase:");

			// read keyword search from user
			String line = br.readLine();
			if (line != null) {
				line = line.trim();
				if (!line.isEmpty()) {
					try {
						// we will use a multi-match query builder that
						// will allow us to match multiple document fields
						// (e.g., search over title and abstract)
						MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(line,
								FieldNames.TITLE.name(), FieldNames.ABSTRACT.name());
						
						// the boost sets a matching word in a title to have
						// a different ranking weight than other fields
						float titleBoost = BOOSTS.get(FieldNames.TITLE.name());
						multiMatchQueryBuilder.field(FieldNames.TITLE.name(), titleBoost);
						
						// TODO: add the abstract field to the fields searched
						float absBoost = BOOSTS.get(FieldNames.ABSTRACT.name());
						multiMatchQueryBuilder.field(FieldNames.ABSTRACT.name(),absBoost);
						
						// here we run the search, specifying how many results
						// we want per "page" of results
						SearchResponse response = client.prepareSearch(indexName).setQuery(multiMatchQueryBuilder) // Query
								.setSize(DOCS_PER_PAGE).setExplain(true).get();
						
						// for each document in the results ...
						for (SearchHit hit : response.getHits().getHits()) {
							// get the JSON data per field
							Map<String, Object> json = hit.getSourceAsMap();

							/*
							String title = (String) json.get(FieldNames.TITLE.name());
							
							//TODO: get and print the title, url and abstract of each result
							String url = (String) json.get(FieldNames.URL.name());
							String abs = (String) json.get(FieldNames.ABSTRACT.name());
							*/

							// NEW: Added because of an error with casting
							Object titleObj = json.get(FieldNames.TITLE.name());
							Object urlObj = json.get(FieldNames.URL.name());
							Object absObj = json.get(FieldNames.ABSTRACT.name());

							String title = titleObj != null ? titleObj.toString() : "";
							String url = urlObj != null ? urlObj.toString() : "";
							String abs = absObj != null ? absObj.toString() : "";
							// ENDNEW

							System.out.println((hit.getIndex()) + "\t" + url + "\t" + title + "\t" + abs + "\t" + hit.getScore());
						}
					} catch (Exception e) {
						System.err.println("Error with query '" + line + "'");
						e.printStackTrace();
					}
				}
			}
		}
	}
}