import java.util.Iterator;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;


public class BlockingQuery {

	public static void main(String[] args) {
	
		
		JsonObject metadata = Json.createObjectBuilder()
			.add("Blocking", 
					Json.createObjectBuilder().
					add("new_data", Json.createObjectBuilder().
							add("tableName", "IPAAS_NEW_PROVIDER")
							.add("Columns", Json.createArrayBuilder().add("col1").add("col2").add("col3")))
							
					.add("master_data", Json.createObjectBuilder().add("tableName", "IPAAS_MASTER_PROVIDER")
							.add("Columns", Json.createArrayBuilder().add("col1").add("col2").add("col3")))
					
					.add("Blocks", Json.createObjectBuilder()
						.add("Block1", Json.createObjectBuilder()
							.add("new_data_table", "IPAAS_NEW_PROVIDER")
							.add("new_data_columns", Json.createArrayBuilder().add("PROV_FIRST_NM").add("PROV_LAST_NM").add("PROV_NPI"))
							.add("master_data_table", "IPAAS_MASTER_PROVIDER")
							.add("master_data_columns", Json.createArrayBuilder().add("PROV_FIRST_NM").add("PROV_LAST_NM").add("PROV_NPI"))		
							.add("block_custom", "AND (new_data_table.patient_gender = master_data_table.patient_gender OR UPPER(master_data_table.patient_gender) = 'U')"))	
							
						.add("Block2", Json.createObjectBuilder()
							.add("new_data_table", "IPAAS_NEW_PROVIDER")
							.add("new_data_columns", Json.createArrayBuilder().add("PROV_FIRST_NM").add("PROV_LAST_NM").add("PROV_NPI"))
							.add("master_data_table", "IPAAS_MASTER_PROVIDER")
							.add("master_data_columns", Json.createArrayBuilder().add("PROV_FIRST_NM").add("PROV_LAST_NM").add("PROV_NPI"))		
							.add("block_custom", "AND (new_data_table.patient_gender = master_data_table.patient_gender OR UPPER(master_data_table.patient_gender) = 'U')")))	
							
							
						.add("outer_block", Json.createObjectBuilder().add("alias_name", "block").add("block_custom", "alias_name where alias_name.l_patient_id > alias_name.r_patient_id;"))	
									
				
				).build();
		
		
		System.out.println(generateBlockingQuery(metadata));
		

	}

	
	public static String generateSelectStatement(JsonObject input){
	
		StringBuilder output = new StringBuilder();
		
		output.append("SELECT ");
		
		
		JsonObject blocking_obj  = input.getJsonObject("Blocking");
	
		JsonObject new_data_obj = blocking_obj.getJsonObject("new_data");
	
		JsonObject master_data_obj =  blocking_obj.getJsonObject("master_data");
		
		
		String new_data_table = new_data_obj.getString("tableName");
		
		String master_data_table = master_data_obj.getString("tableName");
		
		
		JsonArray new_table_columns = new_data_obj.getJsonArray("Columns");
		
		JsonArray master_table_columns = master_data_obj.getJsonArray("Columns");
		
		
		for(JsonValue column : new_table_columns)
			{	
			output.append(new_data_table).append(".").append(column.toString())
			.append(" AS l_").append(column.toString()).append(", \n");

			}
		
		
		for (int i = 0;i<master_table_columns.size();i++){
			
			String column = master_table_columns.get(i).toString();
			
			output.append(master_data_table).append(".").append(column)
			.append(" AS r_").append(column);
			
			
			if(i!=master_table_columns.size()-1)
				output.append(", \n");
			else
				output.append("\n");
	
		}

		
		output.append("FROM ").append(new_data_table).append(" JOIN ").append(master_data_table).append(" ON ");
		
		
		return output.toString();
}
	
	
	public static String generateJoinCondition(JsonObject input){
		
		
		StringBuilder output = new StringBuilder();
			
		String selectStatement = generateSelectStatement(input);
		
		output.append(selectStatement);
				
		JsonObject blocking_obj  = input.getJsonObject("Blocking");
		
		
		JsonObject blocks_obj = blocking_obj.getJsonObject("Blocks");
		
		JsonObject block;
		
		String new_data_table;
		
		String master_data_table;
		
		JsonArray new_data_columns;
		
		JsonArray master_data_columns;
		
		String custom_block;
		
		Set<String> blocks = blocks_obj.keySet();
		
		Iterator<String> itr = blocks.iterator();
		
		int size = blocks.size();
		
		int temp = 0;
	
		while(itr.hasNext()){
			
			String block_num = itr.next();
			
			block = blocks_obj.getJsonObject(block_num);
			
			new_data_table = block.getString("new_data_table");
			
			master_data_table = block.getString("master_data_table");
			
			custom_block = block.getString("block_custom");

			new_data_columns = block.getJsonArray("new_data_columns");
			
			master_data_columns = block.getJsonArray("master_data_columns");
			
			
			
			custom_block = 	custom_block.replace("new_data_table", new_data_table);
			
			custom_block = 	custom_block.replace("master_data_table", master_data_table);
				
			
			for(int i=0;i<new_data_columns.size();i++){
				
				output.append(new_data_table).append(".").append(new_data_columns.get(i).toString())
					  .append(" = ").append(master_data_table).append(".").append(master_data_columns.get(i).toString());
				
				if(i!=new_data_columns.size()-1)
					output.append(" AND ");
				
				else
					output.append(" " + custom_block);
			}
			
			
			if(temp!=size-1){
				output.append("\nUNION ALL \n");
				output.append(selectStatement);
			}
			
			temp++;
	
			
		}
		
		
		return output.toString();
	}
	
	
	public static String generateBlockingQuery(JsonObject input){
		
		
		StringBuilder output = new StringBuilder();
		
		JsonObject blocking_obj  = input.getJsonObject("Blocking");
		
		JsonObject outer_block = blocking_obj.getJsonObject("outer_block");
		
		String alias_name = outer_block.getString("alias_name");
		
		String block_custom = outer_block.getString("block_custom");
		
		
		block_custom = block_custom.replace("alias_name", alias_name);
		
		output.append("SELECT ").append(alias_name).append(".* FROM (")
		.append(generateJoinCondition(input)).append("\n) ").append(block_custom);
		

		return output.toString();
		
	}
	
	
}


