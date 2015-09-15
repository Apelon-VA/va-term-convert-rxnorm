/**
 * Copyright Notice
 *
 * This is a work of the U.S. Government and is not subject to copyright
 * protection in the United States. Foreign copyrights may apply.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gov.va.rxnorm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;

public class DoseFormMapping
{
	
	protected static class DoseForm
	{
		String sctid, tty, rxcui, doseFormString, snomedString, comment;
	}
	
	public static Stream<DoseForm> readDoseFormMapping() throws IOException, URISyntaxException
	{
		return new BufferedReader(new InputStreamReader(DoseFormMapping.class.getResourceAsStream("/10a1 Dose Form Mapping.txt"))).lines()
				.map(line ->{return line.split("\t");})
				.filter(items -> {return (items.length >=4 && StringUtils.isNumeric(items[3]) ? true : false);}).map(items ->
		{
			DoseForm df = new DoseForm();

			df.tty = items[0];
			df.rxcui = items[1];
			df.doseFormString = items[2];
			df.sctid = items[3];
			if (items.length > 4)
			{
				df.snomedString = items[4];
			}
			if (items.length > 5)
			{
				df.comment = items[5];
			}
			return df;
		});
		
	}
}
