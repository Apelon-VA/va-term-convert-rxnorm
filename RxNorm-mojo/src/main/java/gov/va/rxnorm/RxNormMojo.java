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

import gov.va.oia.terminology.converters.sharedUtils.ConsoleUtil;
import gov.va.oia.terminology.converters.sharedUtils.ConverterBaseMojo;
import gov.va.oia.terminology.converters.sharedUtils.EConceptUtility;
import gov.va.oia.terminology.converters.sharedUtils.EConceptUtility.DescriptionType;
import gov.va.oia.terminology.converters.sharedUtils.SCTInfoProvider;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Annotations;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Associations;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_ContentVersion;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Descriptions;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_MemberRefsets;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Relations;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.ConceptCreationNotificationListener;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.Property;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.PropertyAssociation;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.PropertyType;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.ValuePropertyPair;
import gov.va.oia.terminology.converters.sharedUtils.stats.ConverterUUID;
import gov.va.oia.terminology.converters.sharedUtils.umlsUtils.AbbreviationExpansion;
import gov.va.oia.terminology.converters.sharedUtils.umlsUtils.RRFDatabaseHandle;
import gov.va.oia.terminology.converters.sharedUtils.umlsUtils.Relationship;
import gov.va.oia.terminology.converters.sharedUtils.umlsUtils.UMLSFileReader;
import gov.va.oia.terminology.converters.sharedUtils.umlsUtils.ValuePropertyPairWithAttributes;
import gov.va.oia.terminology.converters.sharedUtils.umlsUtils.propertyTypes.PT_Descriptions;
import gov.va.oia.terminology.converters.sharedUtils.umlsUtils.propertyTypes.PT_Refsets;
import gov.va.oia.terminology.converters.sharedUtils.umlsUtils.propertyTypes.PT_Relationship_Metadata;
import gov.va.oia.terminology.converters.sharedUtils.umlsUtils.propertyTypes.PT_SAB_Metadata;
import gov.va.oia.terminology.converters.sharedUtils.umlsUtils.rrf.REL;
import gov.va.oia.terminology.converters.sharedUtils.umlsUtils.sql.TableDefinition;
import gov.va.rxnorm.DoseFormMapping.DoseForm;
import gov.va.rxnorm.propertyTypes.PT_Annotations;
import gov.va.rxnorm.rrf.RXNCONSO;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.ochre.api.component.sememe.version.dynamicSememe.DynamicSememeDataType;
import gov.vha.isaac.ochre.util.UuidT3Generator;
import java.beans.PropertyVetoException;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.dto.TtkConceptChronicle;
import org.ihtsdo.otf.tcc.dto.component.TtkComponentChronicle;
import org.ihtsdo.otf.tcc.dto.component.description.TtkDescriptionChronicle;
import org.ihtsdo.otf.tcc.dto.component.refexDynamic.TtkRefexDynamicMemberChronicle;
import org.ihtsdo.otf.tcc.dto.component.refexDynamic.data.dataTypes.TtkRefexDynamicString;

/**
 * Loader code to convert RxNorm into the workbench.
 */
@Mojo( name = "convert-RxNorm-to-jbin", defaultPhase = LifecyclePhase.PROCESS_SOURCES )
public class RxNormMojo extends ConverterBaseMojo
{
	private final String tablePrefix_ = "RXN";
	private final String sctSab_ = "SNOMEDCT_US";
	private HashMap<String, String> terminologyCodeRefsetPropertyName_ = new HashMap<>();
	
	private BPT_ContentVersion ptContentVersion_;
	
	private PropertyType ptUMLSAttributes_ = new PT_Annotations();
	private PropertyType ptSTypes_;
	private PropertyType ptSuppress_;
	private PropertyType ptLanguages_;
	private PropertyType ptSourceRestrictionLevels_;
	private PropertyType ptSABs_;
	private PropertyType ptRelationshipMetadata_;
	
	private PT_Refsets ptUMLSRefsets_;
	private HashMap<String, PropertyType> ptTermAttributes_ = new HashMap<>();
	private HashMap<String, BPT_MemberRefsets> ptRefsets_ = new HashMap<>();
	private HashMap<String, PropertyType> ptDescriptions_ = new HashMap<>();;
	private HashMap<String, BPT_Relations> ptRelationships_ = new HashMap<>();;
	private HashMap<String, BPT_Associations> ptAssociations_ = new HashMap<>();;

	private HashMap<String, Relationship> nameToRel_ = new HashMap<>();
	private HashMap<String, UUID> semanticTypes_ = new HashMap<>();
	
	private EConceptUtility eConcepts_;
	private RRFDatabaseHandle db_;
	
	private UUID metaDataRoot_;

	private HashSet<UUID> loadedRels_ = new HashSet<>();
	private HashSet<UUID> skippedRels_ = new HashSet<>();
	
	private HashMap<String, AbbreviationExpansion> abbreviationExpansions;
	
	private HashMap<String, Boolean> mapToIsa = new HashMap<>();  //FSN, true or false - true for rel only, false for a rel and association representation
	
	//disabled debug code
	//private HashSet<UUID> conceptUUIDsUsedInRels_ = new HashSet<>();
	//private HashSet<UUID> conceptUUIDsCreated_ = new HashSet<>();

	private TtkConceptChronicle allCUIRefsetConcept_;
	private TtkConceptChronicle cpcRefsetConcept_;
	public static final String cpcRefsetConceptKey_ = "Current Prescribable Content";
	
	private PreparedStatement semanticTypeStatement, conSat, ingredSubstanceMergeCheck, scdProductMergeCheck, cuiRelStatementForward, cuiRelStatementBackward,
		satRelStatement_, hasTTYType_, scdgToSCTIngredient_;
	
	private HashSet<String> allowedCUIsForConcepts, allowedCUIsForRelationships;
	private HashMap<String, Long> allowedSCTTargets;  //Map CUI to SCTID
	private AtomicInteger skippedRelForNotMatchingCUIFilter = new AtomicInteger();
	private AtomicInteger ingredSubstanceMerge_ = new AtomicInteger();
	private AtomicInteger scdProductMerge_ = new AtomicInteger();
	private AtomicInteger ingredSubstanceMergeDupeFail_ = new AtomicInteger();
	private AtomicInteger scdProductMergeDupeFail_ = new AtomicInteger();
	private AtomicInteger convertedTradenameCount_ = new AtomicInteger();
	private AtomicInteger scdgToSCTIngredientCount_ = new AtomicInteger();
	private AtomicInteger doseFormMappedItemsCount_ = new AtomicInteger();
	
	private HashMap<Long, UUID> sctIDToUUID_ = null;
	private HashMap<String, DoseForm> doseFormMappings_ = new HashMap<>();  //rxCUI to DoseForm
	
	/**
	 * An optional list of TTY types which should be included.  If left blank, we create concepts from all CUI's that are in the
	 * SAB RxNorm.  If provided, we only create concepts where the RxCUI has an entry with a TTY that matches one of the TTY's provided here
	 */
	@Parameter (required = false)
	protected List<String> ttyRestriction;
	
	/**
	 * An optional folder that contains the Snomed content (in jbin format).  If provided, all possible linkages to snomed concepts will be 
	 * created.
	 */
	@Parameter (required = false)
	protected File sctInputFileLocation;
	
	
	@Override
	public void execute() throws MojoExecutionException
	{
		try
		{
			outputDirectory.mkdir();

			init();
			
			allCUIRefsetConcept_ = ptUMLSRefsets_.getConcept(ptUMLSRefsets_.CUI_CONCEPTS.getSourcePropertyNameFSN());
			
			cpcRefsetConcept_ = ptRefsets_.get("RXNORM").getConcept(cpcRefsetConceptKey_);

			// Add version data to allRefsetConcept
			eConcepts_.addStringAnnotation(allCUIRefsetConcept_, loaderVersion,  ptContentVersion_.LOADER_VERSION.getUUID(), Status.ACTIVE);
			eConcepts_.addStringAnnotation(allCUIRefsetConcept_, converterResultVersion, ptContentVersion_.RELEASE.getUUID(), Status.ACTIVE);
			
			semanticTypeStatement = db_.getConnection().prepareStatement("select TUI, ATUI, CVF from RXNSTY where RXCUI = ?");
			conSat = db_.getConnection().prepareStatement("select * from RXNSAT where RXCUI = ? and RXAUI = ? and (SAB='RXNORM' or ATN='NDC')");
			
			ingredSubstanceMergeCheck = db_.getConnection().prepareStatement("SELECT DISTINCT r2.code FROM RXNCONSO r1, RXNCONSO r2"
					+ " WHERE r1.RXCUI = ?"
					+ " AND r1.TTY='IN' AND r2.rxcui = r1.rxcui AND r2.sab='" + sctSab_ + "'" 
					+ " AND r2.STR like '% (substance)'");
			
			scdProductMergeCheck = db_.getConnection().prepareStatement("SELECT DISTINCT r2.code FROM RXNCONSO r1, RXNCONSO r2"
					+ " WHERE r1.RXCUI = ?"
					+ " AND r1.TTY='SCD' AND r2.rxcui = r1.rxcui AND r2.sab='" + sctSab_ + "'" 
					+ " AND r2.STR like '% (product)'");
			
			//See doc in addCustomRelationships
			scdgToSCTIngredient_ = db_.getConnection().prepareStatement("SELECT conso_2.code from RXNREL, RXNCONSO as conso_1, RXNCONSO as conso_2" 
					+ " where RXCUI2=? and RELA='has_ingredient'"
					+ " and RXNREL.RXCUI1 = conso_1.RXCUI and conso_1.SAB = 'RXNORM' and conso_1.TTY='IN'"
					+ " and conso_2.RXCUI = RXNREL.RXCUI1 and conso_2.SAB='SNOMEDCT_US' and conso_2.STR like '%(product)' and conso_2.TTY = 'FN'");

			//UMLS and RXNORM do different things with rels - UMLS never has null CUI's, while RxNorm always has null CUI's (when AUI is specified)
			//Also need to join back to MRCONSO to make sure that the target concept is one that we will load with the SAB filter in place.
			cuiRelStatementForward = db_.getConnection().prepareStatement("SELECT distinct r.RXCUI1, r.RXAUI1, r.STYPE1, r.REL, r.RXCUI2, r.RXAUI2, r.STYPE2, "
					+ "r.RELA, r.RUI, r.SRUI, r.SAB, r.SL, r.DIR, r.RG, r.SUPPRESS, r.CVF from RXNREL as r, RXNCONSO "
					+ "WHERE RXCUI2 = ? and RXAUI2 is null and (r.SAB='RXNORM' or r.SAB='" + sctSab_ + "') and r.RXCUI1 = RXNCONSO.RXCUI and "
					+ "(RXNCONSO.SAB='RXNORM' or RXNCONSO.SAB='" + sctSab_ + "')");

			cuiRelStatementBackward = db_.getConnection().prepareStatement("SELECT distinct r.RXCUI1, r.RXAUI1, r.STYPE1, r.REL, r.RXCUI2, r.RXAUI2, r.STYPE2, "
					+ "r.RELA, r.RUI, r.SRUI, r.SAB, r.SL, r.DIR, r.RG, r.SUPPRESS, r.CVF from RXNREL as r, RXNCONSO "
					+ "WHERE RXCUI1 = ? and RXAUI1 is null and (r.SAB='RXNORM' or r.SAB='" + sctSab_ + "') and r.RXCUI2 = RXNCONSO.RXCUI and "
					+ "(RXNCONSO.SAB='RXNORM' or RXNCONSO.SAB='" + sctSab_ + "')");
			
			//Set this to true if you are trying to load on a limited-memory system...
			//ConverterUUID.disableUUIDMap_ = false;
			int cuiCounter = 0;

			Statement statement = db_.getConnection().createStatement();
			
			
			StringBuilder ttyRestrictionQuery = new StringBuilder();
			if (ttyRestriction != null && ttyRestriction.size() > 0)
			{
				ttyRestrictionQuery.append(" and (");
				for (String s : ttyRestriction)
				{
					ttyRestrictionQuery.append("TTY = '");
					ttyRestrictionQuery.append(s);
					ttyRestrictionQuery.append("' or ");
				}
				ttyRestrictionQuery.setLength(ttyRestrictionQuery.length() - " or ".length());
				ttyRestrictionQuery.append(")");
			}
			
			allowedCUIsForConcepts = new HashSet<>();
			ResultSet rs = statement.executeQuery("select RXCUI from RXNCONSO where SAB='RXNORM'" + ttyRestrictionQuery);
			while (rs.next())
			{
				allowedCUIsForConcepts.add(rs.getString("RXCUI"));
			}
			rs.close();
			
			allowedSCTTargets = new HashMap<>();
			HashSet<String> bannedSCTTargets = new HashSet<>();
			if (sctIDToUUID_ != null)
			{
				rs = statement.executeQuery("SELECT DISTINCT RXCUI, CODE from RXNCONSO where SAB='" + sctSab_ + "'");
				while (rs.next())
				{
					String cui = rs.getString("RXCUI");
					long sctid = Long.parseLong(rs.getString("CODE"));
					UUID uuid = sctIDToUUID_.get(sctid);
					if (uuid != null)
					{
						//already have a mapping for this CUI to an SCTID - make sure that it maps to the same UUID
						if (allowedSCTTargets.containsKey(cui) 
								&& allowedSCTTargets.get(cui) != sctid && !uuid.equals(sctIDToUUID_.get(allowedSCTTargets.get(cui))))
						{
							bannedSCTTargets.add(cui);
							allowedSCTTargets.remove(cui);
							ConsoleUtil.println("CUI " + cui + " maps to multiple SCT concepts");
						}
						else if (!bannedSCTTargets.contains(cui))
						{
							//We know there is one and only one mapping from this RxCUI to a concept (that exists) in the version of SCT we are using.
							allowedSCTTargets.put(cui, sctid);
						}
					}
				}
				rs.close();
			}
			ConsoleUtil.println("Allowing " + allowedSCTTargets.size() + " potential relationships to existing SCT concepts");
			ConsoleUtil.println("Not allowing " + bannedSCTTargets.size() + " CUIs to link to SCT concepts because they are mapped to multiple concepts");
			
			allowedCUIsForRelationships = new HashSet<String>();
			allowedCUIsForRelationships.addAll(allowedCUIsForConcepts);
			allowedCUIsForRelationships.addAll(allowedSCTTargets.keySet());
			
			rs = statement.executeQuery("select RXCUI, LAT, RXAUI, SAUI, SCUI, SAB, TTY, CODE, STR, SUPPRESS, CVF from RXNCONSO " 
					+ "where (SAB='RXNORM' or SAB='" + sctSab_ + "') order by RXCUI" );
			
			HashSet<String> skippedCUIForNotMatchingCUIFilter = new HashSet<String>();
			
			ArrayList<RXNCONSO> conceptData = new ArrayList<>();
			while (rs.next())
			{
				RXNCONSO current = new RXNCONSO(rs);
				if (!allowedCUIsForConcepts.contains(current.rxcui))
				{
					skippedCUIForNotMatchingCUIFilter.add(current.rxcui);
					continue;
				}
				if (conceptData.size() > 0 && !conceptData.get(0).rxcui.equals(current.rxcui))
				{
					processCUIRows(conceptData);
					if (cuiCounter % 100 == 0)
					{
						ConsoleUtil.showProgress();
					}
					cuiCounter++;
					if (cuiCounter % 10000 == 0)
					{
						ConsoleUtil.println("Processed " + cuiCounter + " CUIs creating " + eConcepts_.getLoadStats().getConceptCount() + " concepts");
					}
					conceptData.clear();
				}
				
				conceptData.add(current);
			}
			rs.close();
			statement.close();

			// process last
			processCUIRows(conceptData);
			
			ConsoleUtil.println("Processed " + cuiCounter + " CUIs creating " + eConcepts_.getLoadStats().getConceptCount() + " concepts");
			ConsoleUtil.println("Skipped " + skippedCUIForNotMatchingCUIFilter.size() + " concepts for not containing the desired TTY");
			ConsoleUtil.println("Skipped " + skippedRelForNotMatchingCUIFilter + " relationships for linking to a concept we didn't include");

			semanticTypeStatement.close();
			conSat.close();
			ingredSubstanceMergeCheck.close();
			scdProductMergeCheck.close();
			cuiRelStatementForward.close();
			cuiRelStatementBackward.close();
			scdgToSCTIngredient_.close();
			finish();
			
			db_.shutdown();
		}
		catch (Exception e)
		{
			throw new MojoExecutionException("Failure during conversion", e);
		}
	}
	
	/**
	 * Returns the date portion of the file name - so from 'RxNorm_full_09022014.zip' it returns 09022014
	 */
	private String loadDatabase() throws Exception
	{
		// Set up the DB for loading the temp data
		String toReturn = null;
		
		// Read the RRF file directly from the source zip file - need to find the zip first, to get the date out of the file name.
		ZipFile zf = null;
		for (File f : inputFileLocation.listFiles())
		{
			if (f.getName().toLowerCase().startsWith("rxnorm_full_") && f.getName().toLowerCase().endsWith(".zip"))
			{
				zf = new ZipFile(f);
				toReturn = f.getName().substring("rxnorm_full_".length());
				toReturn = toReturn.substring(0, toReturn.length() - 4); 
				break;
			}
		}
		if (zf == null)
		{
			throw new MojoExecutionException("Can't find source zip file");
		}
		
		db_ = new RRFDatabaseHandle();
		File dbFile = new File(outputDirectory, "rrfDB.h2.db");
		boolean createdNew = db_.createOrOpenDatabase(new File(outputDirectory, "rrfDB"));

		if (!createdNew)
		{
			ConsoleUtil.println("Using existing database.  To load from scratch, delete the file '" + dbFile.getAbsolutePath() + ".*'");
		}
		else
		{
			// RxNorm doesn't give us the UMLS tables that define the table definitions, so I put them into an XML file.
			List<TableDefinition> tables = db_.loadTableDefinitionsFromXML(RxNormMojo.class.getResourceAsStream("/RxNormTableDefinitions.xml"));

			for (TableDefinition td : tables)
			{
				ZipEntry ze = zf.getEntry("rrf/" + td.getTableName() + ".RRF");
				if (ze == null)
				{
					throw new MojoExecutionException("Can't find the file 'rrf/" + td.getTableName() + ".RRF' in the zip file");
				}

				db_.loadDataIntoTable(td, new UMLSFileReader(new BufferedReader(new InputStreamReader(zf.getInputStream(ze), "UTF-8"))), null);
			}
			zf.close();

			// Build some indexes to support the queries we will run

			Statement s = db_.getConnection().createStatement();
			ConsoleUtil.println("Creating indexes");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX conso_rxcui_index ON RXNCONSO (RXCUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX conso_rxaui_index ON RXNCONSO (RXAUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sat_rxcui_aui_index ON RXNSAT (RXCUI, RXAUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sat_aui_index ON RXNSAT (RXAUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sty_rxcui_index ON RXNSTY (RXCUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sty_tui_index ON RXNSTY (TUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_rxcui2_index ON RXNREL (RXCUI2, RXAUI2)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_rxaui2_index ON RXNREL (RXCUI1, RXAUI1)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_rela_rel_index ON RXNREL (RELA, REL)");  //helps with rel metadata
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_sab_index ON RXNREL (SAB)");  //helps with rel metadata
			s.close();
		}
		return toReturn;
	}

	private void processCUIRows(ArrayList<RXNCONSO> conceptData) throws IOException, SQLException, PropertyVetoException
	{
		String rxCui = conceptData.get(0).rxcui;
		
		HashSet<String> uniqueTTYs = new HashSet<String>();
		HashSet<String> uniqueSABs = new HashSet<String>();
		
		
		//ensure all the same CUI, gather the TTYs involved
		for (RXNCONSO row : conceptData)
		{
			uniqueTTYs.add(row.tty);
			uniqueSABs.add(row.sab);
			if (!row.rxcui.equals(rxCui))
			{
				throw new RuntimeException("Oops");
			}
		}
		
		TtkConceptChronicle cuiConcept;
		boolean isStub = false;
		
		if (uniqueSABs.size() == 1 && uniqueSABs.iterator().next().equals(sctSab_))
		{
			//This is a SCT only concept - we don't want to create it.  But we might need to put some relationships or associations here.
			//So create a stub concept, then process rels, per normal, at the end.  if we end up putting rels or associations on this 
			//concept, then we will write it out, and it will merge, otherwise, we throw this away.
			isStub = true;
			cuiConcept = eConcepts_.createSkeletonClone(sctIDToUUID_.get(allowedSCTTargets.get(rxCui)));
		}
		else
		{
			UUID cuiBasedUUID = createCUIConceptUUID(rxCui);
			Optional<UUID> mergeOntoSCTConceptUUID = sctMergeCheck(rxCui);
			
			cuiConcept = eConcepts_.createConcept(mergeOntoSCTConceptUUID.isPresent() ? mergeOntoSCTConceptUUID.get() : cuiBasedUUID);
			
			if (mergeOntoSCTConceptUUID.isPresent())
			{
				//Add the UUID we would have generated
				eConcepts_.addUUID(cuiConcept, cuiBasedUUID, ptSABs_.getProperty("RXNORM").getUUID());
			}
			
			eConcepts_.addStringAnnotation(cuiConcept, rxCui, ptUMLSAttributes_.getProperty("RXCUI").getUUID(), Status.ACTIVE);
	
			ArrayList<ValuePropertyPairWithSAB> cuiDescriptions = new ArrayList<>();
			HashMap<String, ValuePropertyPairWithSAB> uniqueCodes = new HashMap<>();
			HashSet<String> sabs = new HashSet<>();
			
			for (RXNCONSO atom : conceptData)
			{
				if (atom.sab.equals(sctSab_))
				{
					continue;
				}
				if (!atom.code.equals("NOCODE") && !uniqueCodes.containsKey(atom.code))
				{
					ValuePropertyPairWithSAB code = new ValuePropertyPairWithSAB(atom.code, ptUMLSAttributes_.getProperty("CODE"), atom.sab);
					code.addUUIDAttribute(ptUMLSAttributes_.getProperty("SAB").getUUID(), ptSABs_.getProperty(atom.sab).getUUID());
					uniqueCodes.put(atom.code, code);
				}
	
				//put it in as a string, so users can search for AUI
				//TODO [remove later] stop doing this when we have a better way to get from AUI on a description to the containing concept
				eConcepts_.addStringAnnotation(cuiConcept, atom.rxaui, ptUMLSAttributes_.getProperty("RXAUI").getUUID(), Status.ACTIVE);
					
				ValuePropertyPairWithSAB desc = new ValuePropertyPairWithSAB(atom.str, ptDescriptions_.get(atom.sab).getProperty(atom.tty), atom.sab);
				
				//used for sorting description to figure out what to use for FSN
				cuiDescriptions.add(desc);
				
				desc.addStringAttribute(ptUMLSAttributes_.getProperty("RXAUI").getUUID(), atom.rxaui);
				desc.addUUIDAttribute(ptUMLSAttributes_.getProperty("SAB").getUUID(), ptSABs_.getProperty(atom.sab).getUUID());
					
				if (atom.saui != null)
				{
					desc.addStringAttribute(ptUMLSAttributes_.getProperty("SAUI").getUUID(), atom.saui);
				}
				if (atom.scui != null)
				{
					desc.addStringAttribute(ptUMLSAttributes_.getProperty("SCUI").getUUID(), atom.scui);
				}
					
				if (atom.suppress != null)
				{
					desc.addUUIDAttribute(ptUMLSAttributes_.getProperty("SUPPRESS").getUUID(), ptSuppress_.getProperty(atom.suppress).getUUID());
				}
					
				if (atom.cvf != null)
				{
					if (atom.cvf.equals("4096"))
					{
						desc.addRefsetMembership(cpcRefsetConcept_);
					}
					else
					{
						throw new RuntimeException("Unexpected value in RXNCONSO cvf column '" + atom.cvf + "'");
					}
				}
				// T-ODO handle language - not currently a 'live' todo, because RxNorm only has ENG at the moment.
				if (!atom.lat.equals("ENG"))
				{
					ConsoleUtil.printErrorln("Non-english lang settings not handled yet!");
				}
				
				//TODO - at this point, sometime in the future, we make make attributes out of the relationships that occur between the AUIs
				//and store them on the descriptions, since OTF doesn't allow relationships between descriptions
	
				sabs.add(atom.sab);
			}
			
			//pulling up unique codes for searchability
			//TODO [remove later], when easier to get from sememe on a description to the concept
			for (ValuePropertyPairWithSAB code : uniqueCodes.values())
			{
				eConcepts_.addStringAnnotation(cuiConcept, code.getValue(), code.getProperty().getUUID(), Status.ACTIVE);
			}
			
			for (String sab : sabs)
			{
				try
				{
					eConcepts_.addDynamicRefsetMember(ptRefsets_.get(sab).getConcept(terminologyCodeRefsetPropertyName_.get(sab)) , cuiConcept.getPrimordialUuid(), null, Status.ACTIVE, null);
				}
				catch (RuntimeException e)
				{
					if (e.toString().contains("duplicate UUID"))
					{
						//ok - this can happen due to multiple merges onto an existing SCT concept
					}
					else
					{
						throw e;
					}
				}
			}
			
			//sanity check on descriptions - make sure we only have one that is of type synonym with the preferred flag
			ArrayList<String> items = new ArrayList<String>();
			for (ValuePropertyPair vpp : cuiDescriptions)
			{
				//Numbers come from the rankings down below in makeDescriptionType(...)
				if (vpp.getProperty().getPropertySubType() >= BPT_Descriptions.SYNONYM && vpp.getProperty().getPropertySubType() <= (BPT_Descriptions.SYNONYM + 20))
				{
					items.add(vpp.getProperty().getSourcePropertyNameFSN() + " " + vpp.getProperty().getPropertySubType());
				}
			}
			
			if (items.size() > 1 && !((items.get(0).endsWith("204") || items.get(0).endsWith("206") ||
					items.get(1).endsWith("204") || items.get(1).endsWith("206"))))
			{
				ConsoleUtil.printErrorln("Need to rank multiple synonym types that are each marked preferred!");
				for (String s : items)
				{
					ConsoleUtil.printErrorln(s);
				}
			}
			
			List<TtkDescriptionChronicle> addedDescriptions = eConcepts_.addDescriptions(cuiConcept, cuiDescriptions);
			
			if (addedDescriptions.size() != cuiDescriptions.size())
			{
				throw new RuntimeException("oops");
			}
			
			HashSet<String> uniqueUMLSCUI = new HashSet<>();
			
			for (int i = 0; i < cuiDescriptions.size(); i++)
			{
				final TtkDescriptionChronicle desc = addedDescriptions.get(i);
				ValuePropertyPairWithSAB descPP = cuiDescriptions.get(i);
				//Add attributes from SAT table
				conSat.clearParameters();
				conSat.setString(1, rxCui);
				conSat.setString(2, descPP.getStringAttribute(ptUMLSAttributes_.getProperty("RXAUI").getUUID()).get(0));  //There be 1 and only 1 of these
				ResultSet rs = conSat.executeQuery();
				
				BiFunction<String, String, Boolean> functions = new BiFunction<String, String, Boolean>()
				{
					@Override
					public Boolean apply(String atn, String atv)
					{
						if ("RXN_OBSOLETED".equals(atn))
						{
							desc.setStatus(Status.INACTIVE);
						}
						//Pull these up to the concept.
						if ("UMLSCUI".equals(atn))
						{
							uniqueUMLSCUI.add(atv);
							return true;
						}
						return false;
					}
				};
		
				processSAT(desc, rs, null, descPP.getSab(), functions);
			}
			
			
			//If all descriptions were inactive, inactivate the concept
			boolean inactivate = true;
			for (TtkDescriptionChronicle d : addedDescriptions)
			{
				if (d.getStatus() == Status.ACTIVE)
				{
					inactivate = false;
					break;
				}
			}
			if (inactivate)
			{
				cuiConcept.getConceptAttributes().setStatus(Status.INACTIVE);
			}
	
			//pulling up the UMLS CUIs.  
			//TODO [remove later] don't need to pull these up, if it is easier to get from sememe on a description to the concept
			//uniqueUMLSCUI is populated during processSAT
			for (String umlsCui : uniqueUMLSCUI)
			{
				UUID itemUUID = ConverterUUID.createNamespaceUUIDFromString("UMLSCUI" + umlsCui);
				eConcepts_.addAnnotation(cuiConcept.getConceptAttributes(), itemUUID, new TtkRefexDynamicString(umlsCui), 
						ptTermAttributes_.get("RXNORM").getProperty("UMLSCUI").getUUID(), Status.ACTIVE, null);
			}
			
			ValuePropertyPairWithAttributes.processAttributes(eConcepts_, cuiDescriptions, addedDescriptions);
			
			//there are no attributes in rxnorm without an AUI.
			
			try
			{
				eConcepts_.addDynamicRefsetMember(allCUIRefsetConcept_, cuiConcept.getPrimordialUuid(), null, Status.ACTIVE, null);
			}
			catch (RuntimeException e)
			{
				if (e.toString().contains("duplicate UUID"))
				{
					//ok - this can happen due to multiple merges onto an existing SCT concept
				}
				else
				{
					throw e;
				}
			}
			
			//add semantic types
			semanticTypeStatement.clearParameters();
			semanticTypeStatement.setString(1, rxCui);
			ResultSet rs = semanticTypeStatement.executeQuery();
			processSemanticTypes(cuiConcept, rs);
			
			addCustomRelationships(rxCui, cuiConcept, conceptData);
		}

		int relCreatedCount = 0;
		cuiRelStatementForward.clearParameters();
		cuiRelStatementForward.setString(1, rxCui);
		relCreatedCount += addRelationships(cuiConcept, REL.read(null, cuiRelStatementForward.executeQuery(), true, allowedCUIsForRelationships, 
				skippedRelForNotMatchingCUIFilter, true, (string -> reverseRel(string))));
		
		cuiRelStatementBackward.clearParameters();
		cuiRelStatementBackward.setString(1, rxCui);
		relCreatedCount += addRelationships(cuiConcept, REL.read(null, cuiRelStatementBackward.executeQuery(), false, allowedCUIsForRelationships, 
				skippedRelForNotMatchingCUIFilter, true, (string -> reverseRel(string))));
		
		//don't write out the stub concept if no rels or associations were added
		if (!isStub || relCreatedCount > 0)
		{
			cuiConcept.writeExternal(dos_);
		}
	}
	
	private void addCustomRelationships(String rxCui, TtkConceptChronicle cuiConcept, ArrayList<RXNCONSO> conceptData) throws SQLException
	{
		/**
		 * Rule 2 is about: 
		 * Create is_a relationship from RxNorm SCDG to SNOMED [ingredient] product concept, 
		 * WHERE no SNOMED equivalent exists per RxNCONSO file for the SCDG.
		 * 
		 * (a) from RxNCONSO identify the SCDG that do NOT have an SCT equivalent concept in the product hierarchy.
		 * (b) from RxNREL identify the IN targets of SCDG (found in (a))  where "has_ingredient" relationship exists.
		 * (c) in RxNCONSO find the SCT product equivalent of the RxCUI TTY=IN found in (a); OR in RxNCONSO find the SCT substance 
		 * equivalent of the RxCUI TTY=IN found in (a), then find the SCT product using the SCT "active ingredient_of" (which is the inverse of 
		 * "has_active_ingredient" relationship) between substance and product.
		 * (d) Create a SOLOR is_a relationship between the SCDG (with no SCT equivalent in RxNCONSO) and the SCT [ingredient type] product concept.
		 * 
		 */
		
		HashSet<String> uniqueTTYs = new HashSet<String>();
		HashSet<String> uniqueSABS = new HashSet<String>();
		for (RXNCONSO x : conceptData)
		{
			uniqueTTYs.add(x.tty);
			uniqueSABS.add(x.sab);
		}
		
		//covers (a)
		if (uniqueTTYs.contains("SCDG") && !uniqueSABS.contains(sctSab_))
		{
			scdgToSCTIngredient_.setString(1, rxCui);
			ResultSet rs = scdgToSCTIngredient_.executeQuery();
			while (rs.next())
			{
				Long sctid = Long.parseLong(rs.getString("code"));
				
				UUID target = sctIDToUUID_.get(sctid);
				
				if (target == null)
				{
					throw new RuntimeException("Unexpected - missing target for sctid " + sctid + " on cui " + rxCui);
				}
				
				eConcepts_.addRelationship(cuiConcept, target);
				scdgToSCTIngredientCount_.incrementAndGet();
			}
		}
	}

	/**
	 * from RxNCONSO find all RxCUI with TTY = IN and SAB = SNOMED CT_US with STR = "*(substance)" 
	 * - that is, all RxCUI with TTY = IN and there is an equivalent SNOMEDCT_US concept in the Substance hierarchy
	 * @return the UUID from the snomed concept that is our merge target (if any)
	 */
	private Optional<UUID> sctMergeCheck(String rxCui) throws SQLException
	{
		if (sctIDToUUID_ == null)
		{
			return Optional.empty();
		}
		
		UUID snoConUUID = null;
		
		if (doseFormMappings_ != null)
		{
			DoseForm df = doseFormMappings_.get(rxCui);
			if (df != null)
			{
				Long id = Long.parseLong(df.sctid);
				snoConUUID = sctIDToUUID_.get(id);
				if (snoConUUID != null)
				{
					doseFormMappedItemsCount_.incrementAndGet();
					return Optional.of(snoConUUID);
				}
			}
		}
		
		ingredSubstanceMergeCheck.setString(1, rxCui);
		
		ResultSet rs = ingredSubstanceMergeCheck.executeQuery();
		while (rs.next())
		{
			long code = Long.parseLong(rs.getString(1));
			UUID found = sctIDToUUID_.get(code);
			if (found != null)
			{
				if (snoConUUID == null || found.equals(snoConUUID))
				{
					if (snoConUUID == null)
					{
						ingredSubstanceMerge_.incrementAndGet();
					}
					snoConUUID = found;
				}
				else 
				{
					ingredSubstanceMergeDupeFail_.incrementAndGet();
					ConsoleUtil.printErrorln("Can't merge ingredient / substance to multiple Snomed concepts: " + rxCui);
				}
			}
			else
			{
				ConsoleUtil.printErrorln("Can't find UUID for SCTID " + code);
			}
		}
		
		rs.close();
		
		scdProductMergeCheck.setString(1, rxCui);
		rs = scdProductMergeCheck.executeQuery();
		boolean passOne = true;
		while (rs.next())
		{
			if (passOne && snoConUUID != null)
			{
				ConsoleUtil.printErrorln("Cant merge to substance and to product at the same time! RXCUI " + rxCui);
				break;
			}
			passOne = false;
			long code = Long.parseLong(rs.getString(1));
			UUID found = sctIDToUUID_.get(code);
			if (found != null)
			{
				if (snoConUUID == null || found.equals(snoConUUID))
				{
					if (snoConUUID == null)
					{
						scdProductMerge_.incrementAndGet();
					}
					snoConUUID = found;
				}
				else 
				{
					scdProductMergeDupeFail_.incrementAndGet();
					ConsoleUtil.printErrorln("Can't merge SCD / product to multiple Snomed concepts: " + rxCui);
				}
			}
			else
			{
				ConsoleUtil.printErrorln("Can't find UUID for SCTID " + code);
			}
		}
		
		
		return Optional.ofNullable(snoConUUID);
	}

	private Property makeDescriptionType(String fsnName, String preferredName, String altName, String description, final Set<String> tty_classes)
	{
		// The current possible classes are:
		// preferred
		// obsolete
		// entry_term
		// hierarchical
		// synonym
		// attribute
		// abbreviation
		// expanded
		// other

		int descriptionTypeRanking;

		//Note - ValuePropertyPairWithSAB overrides the sorting based on these values to kick RXNORM sabs to the top, where 
		//they will get used as FSN.
		if (fsnName.equals("FN") && tty_classes.contains("preferred"))
		{
			descriptionTypeRanking = BPT_Descriptions.FSN;
		}
		else if (fsnName.equals("FN"))
		{
			descriptionTypeRanking = BPT_Descriptions.FSN + 1;
		}
		// preferred gets applied with others as well, in some cases. Don't want 'preferred' 'obsolete' at the top.
		//Just preferred, and we make it the top synonym.
		else if (tty_classes.contains("preferred") && tty_classes.size() == 1)
		{
			//these sub-rankings are somewhat arbitrary at the moment, and in general, unused.  There is an error check up above which 
			//will fail the conversion if it tries to rely on these sub-rankings to find a preferred term
			int preferredSubRank;
			if (altName.equals("IN"))
			{
				preferredSubRank = 1;
			}
			else if (altName.equals("MIN"))
			{
				preferredSubRank = 2;
			}
			else if (altName.equals("PIN"))
			{
				preferredSubRank = 3;
			}
			else if (altName.equals("SCD"))
			{
				preferredSubRank = 4;
			}
			else if (altName.equals("BN"))
			{
				preferredSubRank = 5;
			}
			else if (altName.equals("SBD"))
			{
				preferredSubRank = 6;
			}
			else if (altName.equals("DF"))
			{
				preferredSubRank = 7;
			}
			else if (altName.equals("BPCK"))
			{
				preferredSubRank = 8;
			}
			else if (altName.equals("GPCK"))
			{
				preferredSubRank = 10;
			}
			else if (altName.equals("DFG"))
			{
				preferredSubRank = 11;
			}
			else if (altName.equals("PSN"))
			{
				preferredSubRank = 12;
			}
			else if (altName.equals("SBDC"))
			{
				preferredSubRank = 13;
			}
			else if (altName.equals("SCDC"))
			{
				preferredSubRank = 14;
			}
			else if (altName.equals("SBDF"))
			{
				preferredSubRank = 15;
			}
			else if (altName.equals("SCDF"))
			{
				preferredSubRank = 16;
			}
			else if (altName.equals("SBDG"))
			{
				preferredSubRank = 17;
			}
			else if (altName.equals("SCDG"))
			{
				preferredSubRank = 18;
			}
			else
			{
				preferredSubRank = 20;
				ConsoleUtil.printErrorln("Unranked preferred TTY type! " + fsnName + " " + altName);
			}
			descriptionTypeRanking = BPT_Descriptions.SYNONYM + preferredSubRank;
		}
		else if (tty_classes.contains("entry_term"))
		{
			descriptionTypeRanking = BPT_Descriptions.SYNONYM + 21;
		}
		else if (tty_classes.contains("synonym"))
		{
			descriptionTypeRanking = BPT_Descriptions.SYNONYM + 22;
		}
		else if (tty_classes.contains("expanded"))
		{
			descriptionTypeRanking = BPT_Descriptions.SYNONYM + 23;
		}
		else if (tty_classes.contains("Prescribable Name"))
		{
			descriptionTypeRanking = BPT_Descriptions.SYNONYM + 24;
		}
		else if (tty_classes.contains("abbreviation"))
		{
			descriptionTypeRanking = BPT_Descriptions.SYNONYM + 25;
		}
		else if (tty_classes.contains("attribute"))
		{
			descriptionTypeRanking = BPT_Descriptions.SYNONYM + 26;
		}
		else if (tty_classes.contains("hierarchical"))
		{
			descriptionTypeRanking = BPT_Descriptions.SYNONYM + 27;
		}
		else if (tty_classes.contains("other"))
		{
			descriptionTypeRanking = BPT_Descriptions.SYNONYM + 28;
		}
		else if (tty_classes.contains("obsolete"))
		{
			descriptionTypeRanking = BPT_Descriptions.SYNONYM + 29;
		}
		else
		{
			throw new RuntimeException("Unexpected class type " + Arrays.toString(tty_classes.toArray()));
		}
		return new Property(null, fsnName, preferredName, altName, description, false, descriptionTypeRanking, null);
	}

	private void processSAT(TtkComponentChronicle<?, ?> itemToAnnotate, ResultSet rs, String itemCode, String itemSab, 
			BiFunction<String, String, Boolean> skipCheck) throws SQLException, PropertyVetoException
	{
		while (rs.next())
		{
			//String rxcui = rs.getString("RXCUI");
			String rxaui = rs.getString("RXAUI");
			//String stype = rs.getString("STYPE");
			String code = rs.getString("CODE");
			String atui = rs.getString("ATUI");
			String satui = rs.getString("SATUI");
			String atn = rs.getString("ATN");
			String sab = rs.getString("SAB");
			String atv = rs.getString("ATV");
			String suppress = rs.getString("SUPPRESS");
			String cvf = rs.getString("CVF");
			
			
			if (skipCheck != null)
			{
				if (skipCheck.apply(atn, atv))
				{
					continue;
				}
			}
			
			//for some reason, ATUI isn't always provided - don't know why.  must gen differently in those cases...
			UUID stringAttrUUID;
			UUID refsetUUID = ptTermAttributes_.get("RXNORM").getProperty(atn).getUUID();
			if (atui != null)
			{
				stringAttrUUID = ConverterUUID.createNamespaceUUIDFromString("ATUI" + atui);
			}
			else
			{
				//need to put the aui in here, to keep it unique, as each AUI frequently specs the same CUI
				stringAttrUUID = ConverterUUID.createNamespaceUUIDFromStrings(itemToAnnotate.getPrimordialComponentUuid().toString(), 
						rxaui, atv, refsetUUID.toString());
			}
			
			//You would expect that ptTermAttributes_.get() would be looking up sab, rather than having RxNorm hardcoded... but this is an oddity of 
			//a hack we are doing within the RxNorm load.
			TtkRefexDynamicMemberChronicle attribute = eConcepts_.addAnnotation(itemToAnnotate, stringAttrUUID, new TtkRefexDynamicString(atv), refsetUUID, Status.ACTIVE, null);
			
			if (atui != null)
			{
				eConcepts_.addStringAnnotation(attribute, atui, ptUMLSAttributes_.getProperty("ATUI").getUUID(), null);
			}
			
			//dropping for space savings
//			if (stype != null)
//			{
//				eConcepts_.addUuidAnnotation(attribute, ptSTypes_.getProperty(stype).getUUID(), ptUMLSAttributes_.getProperty("STYPE").getUUID());
//			}
			
			if (code != null && itemCode != null && !code.equals(itemCode))
			{
				throw new RuntimeException("oops");
//				if ()
//				{
//					eConcepts_.addStringAnnotation(attribute, code, ptUMLSAttributes_.getProperty("CODE").getUUID(), Status.ACTIVE);
//				}
			}

			if (satui != null)
			{
				eConcepts_.addStringAnnotation(attribute, satui, ptUMLSAttributes_.getProperty("SATUI").getUUID(), Status.ACTIVE);
			}
			
			//only load the sab if it is different than the sab of the item we are putting this attribute on
			if (sab != null && !sab.equals(itemSab))
			{
				throw new RuntimeException("Oops");
				//eConcepts_.addUuidAnnotation(attribute, ptSABs_.getProperty(sab).getUUID(), ptUMLSAttributes_.getProperty("SAB").getUUID());
			}
			if (suppress != null)
			{
				eConcepts_.addUuidAnnotation(attribute, ptSuppress_.getProperty(suppress).getUUID(), ptUMLSAttributes_.getProperty("SUPPRESS").getUUID());
			}
			if (cvf != null)
			{
				if (cvf.equals("4096"))
				{
					eConcepts_.addDynamicRefsetMember(cpcRefsetConcept_, attribute.getPrimordialComponentUuid(), null, Status.ACTIVE, null);
				}
				else
				{
					throw new RuntimeException("Unexpected value in RXNSAT cvf column '" + cvf + "'");
				}
			}
		}

		rs.close();
	}
	
	
	/**
	 * If sabList is null or empty, no sab filtering is done. 
	 */
	private void init() throws Exception
	{
		clearTargetFiles();
		
		String fileNameDatePortion = loadDatabase();
		SimpleDateFormat sdf = new SimpleDateFormat("MMddyyyy");
		long defaultTime = sdf.parse(fileNameDatePortion).getTime();
		
		abbreviationExpansions = AbbreviationExpansion.load(getClass().getResourceAsStream("/RxNormAbbreviationsExpansions.txt"));
		
		mapToIsa.put("isa", true);
		mapToIsa.put("inverse_isa", true);
		//not translating this one to isa for now
		//		mapToIsa.add("CHD");
		mapToIsa.put("tradename_of", false);
		mapToIsa.put("has_tradename", false);
		
		
		File binaryOutputFile = new File(outputDirectory, "RRF-" + tablePrefix_ + ".jbin");

		dos_ = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(binaryOutputFile)));
		eConcepts_ = new EConceptUtility(IsaacMetadataAuxiliaryBinding.RXNORM.getPrimodialUuid(), dos_, defaultTime);

		metaDataRoot_ = ConverterUUID.createNamespaceUUIDFromString("metadata");
		eConcepts_.createAndStoreMetaDataConcept(metaDataRoot_, "RxNorm RRF Metadata", IsaacMetadataAuxiliaryBinding.ISAAC_ROOT.getPrimodialUuid(), null, dos_);

		loadMetaData();

		ConsoleUtil.println("Metadata Statistics");
		for (String s : eConcepts_.getLoadStats().getSummary())
		{
			ConsoleUtil.println(s);
		}

		eConcepts_.clearLoadStats();
		satRelStatement_ = db_.getConnection().prepareStatement("select * from " + tablePrefix_ + "SAT where RXAUI" 
				+ "= ? and STYPE='RUI' and (SAB='RXNORM' or SAB='" + sctSab_ + "')");
		
		hasTTYType_ = db_.getConnection().prepareStatement("select count (*) as count from RXNCONSO where rxcui=? and TTY=? and SAB='RXNORM'");
		
		if (sctInputFileLocation != null && sctInputFileLocation.isDirectory())
		{
			sctIDToUUID_ = SCTInfoProvider.readSCTIDtoUUIDMapInfo(sctInputFileLocation);
		}
		
		ConsoleUtil.println("Reading dose form mapping file");
		DoseFormMapping.readDoseFormMapping().forEach(df ->
		{
			doseFormMappings_.put(df.rxcui, df);
		});
		ConsoleUtil.println("Read " + doseFormMappings_.size() + " dose form mappings");
	}
	
	private void finish() throws IOException, SQLException
	{
		checkRelationships();
		satRelStatement_.close();
		hasTTYType_.close();
		eConcepts_.storeRefsetConcepts(ptUMLSRefsets_, dos_);
		for (BPT_MemberRefsets r : ptRefsets_.values())
		{
			eConcepts_.storeRefsetConcepts(r, dos_);
		}

		dos_.close();
		ConsoleUtil.println("Load Statistics");
		for (String s : eConcepts_.getLoadStats().getSummary())
		{
			ConsoleUtil.println(s);
		}
		
		ConsoleUtil.println("Ingredient / Substance merge concepts: " + ingredSubstanceMerge_.get());
		ConsoleUtil.println("Ingredient / Substance merge fail due to duplicates: " + ingredSubstanceMergeDupeFail_.get());
		ConsoleUtil.println("SCD / Product merge concepts: " + scdProductMerge_.get());
		ConsoleUtil.println("SCD / Product merge fail due to duplicates: " + scdProductMergeDupeFail_.get());
		ConsoleUtil.println("Dose Form merge concepts: " + doseFormMappedItemsCount_.get());
		ConsoleUtil.println("Converted tradename of relationships: " + convertedTradenameCount_.get());
		ConsoleUtil.println("Added is a relationships for scdg to ingredient: " + scdgToSCTIngredientCount_.get());

		// this could be removed from final release. Just added to help debug editor problems.
		ConsoleUtil.println("Dumping UUID Debug File");
		ConverterUUID.dump(outputDirectory, "RxNormUUID");
		ConsoleUtil.writeOutputToFile(new File(outputDirectory, "ConsoleOutput.txt").toPath());
	}
	
	private void clearTargetFiles()
	{
		new File(outputDirectory, "RxNormUUIDDebugMap.txt").delete();
		new File(outputDirectory, "ConsoleOutput.txt").delete();
		new File(outputDirectory, "RRF.jbin").delete();
	}
	
	private void loadMetaData() throws Exception
	{
		ptUMLSRefsets_ = new PT_Refsets("RxNorm RRF");
		ptContentVersion_ = new BPT_ContentVersion();
		final PropertyType sourceMetadata = new PT_SAB_Metadata();
		ptRelationshipMetadata_ = new PT_Relationship_Metadata();

		//don't load ptContentVersion_ yet - custom code might add to it
		eConcepts_.loadMetaDataItems(Arrays.asList(ptUMLSRefsets_, sourceMetadata, ptRelationshipMetadata_, ptUMLSAttributes_),
				metaDataRoot_, dos_);
		
		loadTerminologySpecificMetadata();
		
		//STYPE values
		ptSTypes_= new PropertyType("STYPEs", true, DynamicSememeDataType.STRING){};
		{
			ConsoleUtil.println("Creating STYPE types");
			ptSTypes_.indexByAltNames();
			Statement s = db_.getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT DISTINCT VALUE, TYPE, EXPL FROM " + tablePrefix_ + "DOC where DOCKEY like 'STYPE%'");
			while (rs.next())
			{
				String sType = rs.getString("VALUE");
				String type = rs.getString("TYPE");
				String name = rs.getString("EXPL");

				if (!type.equals("expanded_form"))
				{
					throw new RuntimeException("Unexpected type in the attribute data within DOC: '" + type + "'");
				}				
				
				ptSTypes_.addProperty(name, null, sType, null);
			}
			rs.close();
			s.close();
		}
		eConcepts_.loadMetaDataItems(ptSTypes_, metaDataRoot_, dos_);
		
		
		ptSuppress_=  xDocLoaderHelper("SUPPRESS", "Suppress", false);
		
		//Not yet loading co-occurrence data yet, so don't need these yet.
		//xDocLoaderHelper("COA", "Attributes of co-occurrence", false);
		//xDocLoaderHelper("COT", "Type of co-occurrence", true);  
		
		final PropertyType contextTypes = xDocLoaderHelper("CXTY", "Context Type", false);
		
		//not yet loading mappings - so don't need this yet
		//xDocLoaderHelper("FROMTYPE", "Mapping From Type", false);  
		//xDocLoaderHelper("TOTYPE", "Mapping To Type", false);  
		//MAPATN - not yet used in UMLS
		
		// Handle the languages
		{
			ConsoleUtil.println("Creating language types");
			ptLanguages_ = new PropertyType("Languages", true, DynamicSememeDataType.STRING){};
			Statement s = db_.getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT * from " + tablePrefix_ + "DOC where DOCKEY = 'LAT' and VALUE in (select distinct LAT from " 
					+ tablePrefix_ + "CONSO where SAB='RXNORM')");
			while (rs.next())
			{
				String abbreviation = rs.getString("VALUE");
				String type = rs.getString("TYPE");
				String expansion = rs.getString("EXPL");

				if (!type.equals("expanded_form"))
				{
					throw new RuntimeException("Unexpected type in the language data within DOC: '" + type + "'");
				}

				Property p = ptLanguages_.addProperty(abbreviation, expansion, null);

				if (abbreviation.equals("ENG") || abbreviation.equals("SPA"))
				{
					// use official ISAAC languages
					if (abbreviation.equals("ENG"))
					{
						p.setWBPropertyType(IsaacMetadataAuxiliaryBinding.ENGLISH.getPrimodialUuid());
					}
					else if (abbreviation.equals("SPA"))
					{
						p.setWBPropertyType(IsaacMetadataAuxiliaryBinding.SPANISH.getPrimodialUuid());
					}
					else
					{
						throw new RuntimeException("oops");
					}
				}
			}
			rs.close();
			s.close();
			eConcepts_.loadMetaDataItems(ptLanguages_, metaDataRoot_, dos_);
		}
		
		// And Source Restriction Levels
		{
			ConsoleUtil.println("Creating Source Restriction Level types");
			ptSourceRestrictionLevels_ = new PropertyType("Source Restriction Levels", true, DynamicSememeDataType.UUID){};
			PreparedStatement ps = db_.getConnection().prepareStatement("SELECT VALUE, TYPE, EXPL from " + tablePrefix_ + "DOC where DOCKEY=? ORDER BY VALUE");
			ps.setString(1, "SRL");
			ResultSet rs = ps.executeQuery();
			
			String value = null;
			String description = null;
			String uri = null;
			
			//Two entries per SRL, read two rows, create an entry.
			
			while (rs.next())
			{
				String type = rs.getString("TYPE");
				String expl = rs.getString("EXPL");
				
				if (type.equals("expanded_form"))
				{
					description = expl;
				}
				else if (type.equals("uri"))
				{
					uri = expl;
				}
				else
				{
					throw new RuntimeException("oops");
				}
					
				
				if (value == null)
				{
					value = rs.getString("VALUE");
				}
				else
				{
					if (!value.equals(rs.getString("VALUE")))
					{
						throw new RuntimeException("oops");
					}
					
					if (description == null || uri == null)
					{
						throw new RuntimeException("oops");
					}
					
					Property p = ptSourceRestrictionLevels_.addProperty(value, null, description);
					final String temp = uri;
					p.registerConceptCreationListener(new ConceptCreationNotificationListener()
					{
						@Override
						public void conceptCreated(Property property, TtkConceptChronicle concept)
						{
							eConcepts_.addStringAnnotation(concept, temp, ptUMLSAttributes_.getProperty("URI").getUUID(), Status.ACTIVE);
						}
					});
					type = null;
					expl = null;
					value = null;
				}
			}
			rs.close();
			ps.close();

			eConcepts_.loadMetaDataItems(ptSourceRestrictionLevels_, metaDataRoot_, dos_);
		}

		// And Source vocabularies
		final PreparedStatement getSABMetadata = db_.getConnection().prepareStatement("Select * from " + tablePrefix_ + "SAB where (VSAB = ? or (RSAB = ? and CURVER='Y' ))");
		{
			ConsoleUtil.println("Creating Source Vocabulary types");
			ptSABs_ = new PropertyType("Source Vocabularies", true, DynamicSememeDataType.STRING){};
			ptSABs_.indexByAltNames();
			
			HashSet<String> sabList = new HashSet<>();
			sabList.add("RXNORM");
			
			Statement s = db_.getConnection().createStatement();
			ResultSet rs = s.executeQuery("select distinct SAB from RXNSAT where ATN='NDC'");
			while (rs.next())
			{
				sabList.add(rs.getString("SAB"));
			}
			rs.close();
			s.close();
			
			for (String currentSab : sabList)
			{
				s = db_.getConnection().createStatement();
				rs = s.executeQuery("SELECT SON from " + tablePrefix_ + "SAB WHERE (VSAB='" + currentSab + "' or (RSAB='" + currentSab + "' and CURVER='Y'))");
				if (rs.next())
				{
					String son = rs.getString("SON");

					Property p = ptSABs_.addProperty(son, null, currentSab, null);
					p.registerConceptCreationListener(new ConceptCreationNotificationListener()
					{
						@Override
						public void conceptCreated(Property property, TtkConceptChronicle concept)
						{
							try
							{
								//lookup the other columns for the row with this newly added RSAB terminology
								getSABMetadata.setString(1, property.getSourcePropertyAltName() == null ? property.getSourcePropertyNameFSN() : property.getSourcePropertyAltName());
								getSABMetadata.setString(2, property.getSourcePropertyAltName() == null ? property.getSourcePropertyNameFSN() : property.getSourcePropertyAltName());
								ResultSet rs2 = getSABMetadata.executeQuery();
								if (rs2.next())  //should be only one result
								{
									for (Property metadataProperty : sourceMetadata.getProperties())
									{
										String columnName = metadataProperty.getSourcePropertyAltName() == null ? metadataProperty.getSourcePropertyNameFSN() 
												: metadataProperty.getSourcePropertyAltName();
										String columnValue = rs2.getString(columnName);
										if (columnValue == null)
										{
											continue;
										}
										if (columnName.equals("SRL"))
										{
											eConcepts_.addUuidAnnotation(concept, ptSourceRestrictionLevels_.getProperty(columnValue).getUUID(),
													metadataProperty.getUUID());
										}
										else if (columnName.equals("CXTY"))
										{
											eConcepts_.addUuidAnnotation(concept, contextTypes.getProperty(columnValue).getUUID(),
													metadataProperty.getUUID());
										}
										else
										{
											eConcepts_.addStringAnnotation(concept, columnValue, metadataProperty.getUUID(), Status.ACTIVE);
										}
									}
								}
								if (rs2.next())
								{
									throw new RuntimeException("Too many sabs.  Perhaps you should be using versioned sabs!");
								}
								rs2.close();
							}
							catch (SQLException e)
							{
								throw new RuntimeException("Error loading *SAB", e);
							}
						}
					});
				}
				else
				{
					throw new RuntimeException("Too few? SABs - perhaps you need to use versioned SABs.");
				}
				if (rs.next())
				{
					throw new RuntimeException("Too many SABs for '" + currentSab  + "' - perhaps you need to use versioned SABs.");
				}
				rs.close();
				s.close();
			}
			eConcepts_.loadMetaDataItems(ptSABs_, metaDataRoot_, dos_);
			getSABMetadata.close();
		}

		// And semantic types
		{
			ConsoleUtil.println("Creating semantic types");
			PropertyType ptSemanticTypes = new PropertyType("Semantic Types", true, DynamicSememeDataType.UUID){};
			Statement s = db_.getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT distinct TUI, STN, STY from " + tablePrefix_+ "STY");
			while (rs.next())
			{
				final String tui = rs.getString("TUI");
				final String stn = rs.getString("STN");
				String sty = rs.getString("STY");

				Property p = ptSemanticTypes.addProperty(sty);
				semanticTypes_.put(tui, p.getUUID());
				p.registerConceptCreationListener(new ConceptCreationNotificationListener()
				{
					@Override
					public void conceptCreated(Property property, TtkConceptChronicle concept)
					{
						eConcepts_.addStringAnnotation(concept, tui, ptUMLSAttributes_.getProperty("TUI").getUUID(), Status.ACTIVE);
						eConcepts_.addStringAnnotation(concept, stn, ptUMLSAttributes_.getProperty("STN").getUUID(), Status.ACTIVE);
					}
				});
			}
			rs.close();
			s.close();

			eConcepts_.loadMetaDataItems(ptSemanticTypes, metaDataRoot_, dos_);
		}
		
		eConcepts_.loadMetaDataItems(ptContentVersion_, metaDataRoot_, dos_);
	}
	
	/*
	 * Note - may return null, if there were no instances of the requested data
	 */
	private PropertyType xDocLoaderHelper(String dockey, String niceName, boolean loadAsDefinition) throws Exception
	{
		ConsoleUtil.println("Creating '" + niceName + "' types");
		PropertyType pt = new PropertyType(niceName, true, DynamicSememeDataType.UUID) {};
		{
			if (!loadAsDefinition)
			{
				pt.indexByAltNames();
			}
			Statement s = db_.getConnection().createStatement();
			ResultSet rs = s.executeQuery("SELECT VALUE, TYPE, EXPL FROM " + tablePrefix_ + "DOC where DOCKEY='" + dockey + "'");
			while (rs.next())
			{
				String value = rs.getString("VALUE");
				String type = rs.getString("TYPE");
				String name = rs.getString("EXPL");
				
				if (value == null)
				{
					//there is a null entry, don't care about it.
					continue;
				}

				if (!type.equals("expanded_form"))
				{
					throw new RuntimeException("Unexpected type in the attribute data within DOC: '" + type + "'");
				}				
				
				pt.addProperty((loadAsDefinition ? value : name), null, (loadAsDefinition ? null : value), (loadAsDefinition ? name : null));
			}
			rs.close();
			s.close();
		}
		if (pt.getProperties().size() == 0)
		{
			//This can happen, depending on what is included during the metamorphosys run
			ConsoleUtil.println("No entries found for '" + niceName + "' - skipping");
			return null;
		}
		eConcepts_.loadMetaDataItems(pt, metaDataRoot_, dos_);
		return pt;
	}
	
	private void loadTerminologySpecificMetadata() throws Exception
	{
		UUID mainNamespace = ConverterUUID.getNamespace();
		
		String[] sabs = new String[] {"RXNORM"};
		
		for (String sab : sabs)
		{
			UUID termSpecificMetadataRoot;
			String terminologyName;
			
			ConsoleUtil.println("Setting up metadata for " + sab);
			
			terminologyName = "RxNorm";
			//Change to a different namespace, so property types that are repeated in the metadata don't collide.
			ConverterUUID.configureNamespace(ConverterUUID.createNamespaceUUIDFromString(eConcepts_.moduleUuid_, terminologyName + ".metadata"));

			termSpecificMetadataRoot = ConverterUUID.createNamespaceUUIDFromString("metadata");
			
			eConcepts_.createAndStoreMetaDataConcept(termSpecificMetadataRoot, terminologyName + " Metadata", 
					IsaacMetadataAuxiliaryBinding.ISAAC_ROOT.getPrimodialUuid(), null, dos_);
			
			//dynamically add more attributes from *DOC
			{
				ConsoleUtil.println("Creating attribute types");
				PropertyType annotations = new BPT_Annotations() {};
				annotations.indexByAltNames();
				
				Statement s = db_.getConnection().createStatement();
				//extra logic at the end to keep NDC's from any sab when processing RXNorm
				ResultSet rs = s.executeQuery("SELECT VALUE, TYPE, EXPL from " + tablePrefix_ + "DOC where DOCKEY = 'ATN' and VALUE in (select distinct ATN from " 
						+ tablePrefix_ + "SAT" + " where SAB='" + sab + "' or ATN='NDC')");
				while (rs.next())
				{
					String abbreviation = rs.getString("VALUE");
					String type = rs.getString("TYPE");
					String expansion = rs.getString("EXPL");
	
					if (!type.equals("expanded_form"))
					{
						throw new RuntimeException("Unexpected type in the attribute data within DOC: '" + type + "'");
					}
	
					String preferredName = null;
					String description = null;
					if (expansion.length() > 30)
					{
						description = expansion;
					}
					else
					{
						preferredName = expansion;
					}
					
					AbbreviationExpansion ae = abbreviationExpansions.get(abbreviation);
					if (ae == null)
					{
						ConsoleUtil.printErrorln("No Abbreviation Expansion found for " + abbreviation);
						annotations.addProperty(abbreviation, preferredName, description);
					}
					else
					{
						annotations.addProperty(ae.getExpansion(), null, ae.getAbbreviation(), ae.getDescription());
					}
				}

				rs.close();
				s.close();
				
				if (annotations.getProperties().size() > 0)
				{
					eConcepts_.loadMetaDataItems(annotations, termSpecificMetadataRoot, dos_);
				}
				ptTermAttributes_.put(sab, annotations);
			}
			
			
			// And Descriptions
			{
				ConsoleUtil.println("Creating description_ types");
				PropertyType descriptions = new PT_Descriptions(terminologyName);
				descriptions.indexByAltNames();
				Statement s = db_.getConnection().createStatement();
				ResultSet usedDescTypes;
				usedDescTypes = s.executeQuery("select distinct TTY from RXNCONSO WHERE SAB='" + sab + "'");

				PreparedStatement ps = db_.getConnection().prepareStatement("select TYPE, EXPL from " + tablePrefix_ + "DOC where DOCKEY='TTY' and VALUE=?");

				while (usedDescTypes.next())
				{
					String tty = usedDescTypes.getString(1);
					ps.setString(1, tty);
					ResultSet descInfo = ps.executeQuery();

					String expandedForm = null;
					final HashSet<String> classes = new HashSet<>();

					while (descInfo.next())
					{
						String type = descInfo.getString("TYPE");
						String expl = descInfo.getString("EXPL");
						if (type.equals("expanded_form"))
						{
							if (expandedForm != null)
							{
								throw new RuntimeException("Expected name to be null!");
							}
							expandedForm = expl;
						}
						else if (type.equals("tty_class"))
						{
							classes.add(expl);
						}
						else
						{
							throw new RuntimeException("Unexpected type in DOC for '" + tty + "'");
						}
					}
					descInfo.close();
					ps.clearParameters();
					
					Property p = null;
					AbbreviationExpansion ae = abbreviationExpansions.get(tty);
					if (ae == null)
					{
						ConsoleUtil.printErrorln("No Abbreviation Expansion found for " + tty);
						p = makeDescriptionType(tty, expandedForm, null, null, classes);
					}
					else
					{
						p = makeDescriptionType(ae.getExpansion(), null, ae.getAbbreviation(), ae.getDescription(), classes);
					}
					
					descriptions.addProperty(p);
					p.registerConceptCreationListener(new ConceptCreationNotificationListener()
					{
						@Override
						public void conceptCreated(Property property, TtkConceptChronicle concept)
						{
							for (String tty_class : classes)
							{
								eConcepts_.addStringAnnotation(concept, tty_class, ptUMLSAttributes_.getProperty("tty_class").getUUID(), Status.ACTIVE);
							}
						}
					});
					
				}
				usedDescTypes.close();
				s.close();
				ps.close();
				
				ptDescriptions_.put(sab, descriptions);
				
				if (descriptions.getProperties().size() > 0)
				{
					eConcepts_.loadMetaDataItems(descriptions, termSpecificMetadataRoot, dos_);
				}
			}
			
			//Make a refset
			BPT_MemberRefsets refset = new BPT_MemberRefsets(terminologyName);
			terminologyCodeRefsetPropertyName_.put(sab, "All " + terminologyName + " Concepts");
			refset.addProperty(terminologyCodeRefsetPropertyName_.get(sab));
			refset.addProperty(cpcRefsetConceptKey_);
			ptRefsets_.put(sab, refset);
			eConcepts_.loadMetaDataItems(refset, termSpecificMetadataRoot, dos_);
			
			loadRelationshipMetadata(terminologyName, sab, termSpecificMetadataRoot);
		}
		//Go back to the primary namespace
		ConverterUUID.configureNamespace(mainNamespace);
	}
	
	private void loadRelationshipMetadata(String terminologyName, String sab, UUID terminologyMetadataRoot) throws Exception
	{
		ConsoleUtil.println("Creating relationship types");
		//Both of these get added as extra attributes on the relationship definition
		HashMap<String, ArrayList<String>> snomedCTRelaMappings = new HashMap<>(); //Maps something like 'has_specimen_source_morphology' to '118168003' (may be more than one target SCT code)
		HashMap<String, String> snomedCTRelMappings = new HashMap<>();  //Maps something like '118168003' to 'RO'
		
		nameToRel_ = new HashMap<>();
		
		Statement s = db_.getConnection().createStatement();
		//get the inverses of first, before the expanded forms
		ResultSet rs = s.executeQuery("SELECT DOCKEY, VALUE, TYPE, EXPL FROM " + tablePrefix_ + "DOC where DOCKEY ='REL' or DOCKEY = 'RELA' order by TYPE DESC ");
		while (rs.next())
		{
			String dockey = rs.getString("DOCKEY");
			String value = rs.getString("VALUE");
			String type = rs.getString("TYPE");
			String expl = rs.getString("EXPL");
			if (value == null)
			{
				continue;  //don't need this one
			}
			
			if (type.equals("snomedct_rela_mapping"))
			{
				ArrayList<String> targetSCTIDs = snomedCTRelaMappings.get(expl);
				if (targetSCTIDs == null)
				{
					targetSCTIDs = new ArrayList<String>();
					snomedCTRelaMappings.put(expl, targetSCTIDs);
				}
				targetSCTIDs.add(value);
			}
			else if (type.equals("snomedct_rel_mapping"))
			{
				snomedCTRelMappings.put(value, expl);
			}
			else
			{
				Relationship rel = nameToRel_.get(value);
				if (rel == null)
				{
					if (type.endsWith("_inverse"))
					{
						rel = nameToRel_.get(expl);
						if (rel == null)
						{
							rel = new Relationship(dockey.equals("RELA"));
							nameToRel_.put(value, rel);
							nameToRel_.put(expl, rel);
						}
						else
						{
							throw new RuntimeException("shouldn't happen due to query order");
						}
					}
					else
					{
						//only cases where there is no inverse
						rel = new Relationship(dockey.equals("RELA"));
						nameToRel_.put(value, rel);
					}
				}
				
				if (type.equals("expanded_form"))
				{
					rel.addDescription(value, expl);
				}
				else if (type.equals("rela_inverse") || type.equals("rel_inverse"))
				{
					rel.addRelInverse(value, expl);
				}
				else
				{
					throw new RuntimeException("Oops");
				}
			}
		}
		
		rs.close();
		s.close();
		
		HashSet<String> actuallyUsedRelsOrRelas = new HashSet<>();
		
		for (Entry<String, ArrayList<String>> x : snomedCTRelaMappings.entrySet())
		{
			if (!nameToRel_.containsKey(x.getKey()))
			{
				//metamorphosys doesn't seem to remove these when the sct rel types aren't included - just silently remove them 
				//unless it seems that they should map.
				//may_be_a appears to be a bug in RxNorm 2013-12-02.  silently ignore...
				//TODO see if they fix it in the future, make this check version specific?
				//seems to be getting worse... now it fails to remove 'has_life_circumstance' too in 2014AA, and a few others.
				//Changing to a warning.
				ConsoleUtil.printErrorln("Warning - The 'snomedct_rela_mapping' '" + x.getKey() + "' does not have a corresponding REL entry!  Skipping");
//				if (!x.getKey().equals("may_be_a") && !x.getKey().equals("has_life_circumstance"))
//				{
//					throw new RuntimeException("ERROR - No rel for " + x.getKey() + ".");
//				}
				for (String sctId : x.getValue())
				{
					snomedCTRelMappings.remove(sctId);
				}
			}
			else
			{
				for (String sctid : x.getValue())
				{
					nameToRel_.get(x.getKey()).addSnomedCode(x.getKey(), sctid);
					String relType = snomedCTRelMappings.remove(sctid);
					if (relType != null)
					{
						nameToRel_.get(x.getKey()).addRelType(x.getKey(), relType);
						//Shouldn't need this, but there are some cases where the metadata is inconsistent - with how it is actually used.
						actuallyUsedRelsOrRelas.add(relType);
					}
				}
			}
		}
		
		if (snomedCTRelMappings.size() > 0)
		{
			for (Entry<String, String> x : snomedCTRelMappings.entrySet())
			{
				ConsoleUtil.printErrorln(x.getKey() + ":" + x.getValue());
			}
			throw new RuntimeException("oops - still have (things listed above)");
			
		}
		
		final BPT_Relations relationships = new BPT_Relations(terminologyName) {};  
		relationships.indexByAltNames();
		final BPT_Associations associations = new BPT_Associations(terminologyName) {};
		associations.indexByAltNames();
		
		s = db_.getConnection().createStatement();
		rs = s.executeQuery("select distinct REL, RELA from " + tablePrefix_ + "REL where SAB='" + sab + "'");
		while (rs.next())
		{
			actuallyUsedRelsOrRelas.add(rs.getString("REL"));
			if (rs.getString("RELA") != null)
			{
				actuallyUsedRelsOrRelas.add(rs.getString("RELA"));
			}
		}
		rs.close();
		s.close();
		
		HashSet<Relationship> uniqueRels = new HashSet<>(nameToRel_.values());
		for (final Relationship r : uniqueRels)
		{
			r.setSwap(db_.getConnection(), tablePrefix_);
			
			if (!actuallyUsedRelsOrRelas.contains(r.getFSNName()) && !actuallyUsedRelsOrRelas.contains(r.getInverseFSNName()))
			{
				continue;
			}
			
			Property p = null;
			Boolean relTypeMap = mapToIsa.get(r.getFSNName());
			if (relTypeMap != null)  //true or false, make it a rel
			{
				p = new Property((r.getAltName() == null ? r.getFSNName() : r.getAltName()), null, (r.getAltName() == null ? null : r.getFSNName()),
						r.getDescription(), EConceptUtility.isARelUuid_);  //map to isA
				relationships.addProperty(p);  //conveniently, the only thing we will treat as relationships are things mapped to isa.
			}
			if (relTypeMap == null || relTypeMap == false)  //don't make it an association if set to true
			{
				p = new PropertyAssociation(null, (r.getAltName() == null ? r.getFSNName() : r.getAltName()), 
						(r.getAltName() == null ? null : r.getFSNName()), (r.getInverseAltName() == null ? r.getInverseFSNName() : r.getInverseAltName()),
						r.getDescription(), false);
				associations.addProperty(p);
			}
			
			p.registerConceptCreationListener(new ConceptCreationNotificationListener()
			{
				@Override
				public void conceptCreated(Property property, TtkConceptChronicle concept)
				{
					//associations already handle inverse names 
					if (!(property instanceof PropertyAssociation) && r.getInverseFSNName() != null)
					{
						eConcepts_.addDescription(concept, (r.getInverseAltName() == null ? r.getInverseFSNName() : r.getInverseAltName()), DescriptionType.FSN, 
								false, ptDescriptions_.get(sab).getProperty("Inverse FSN").getUUID(),
								ptDescriptions_.get(sab).getProperty("Inverse FSN").getPropertyType().getPropertyTypeReferenceSetUUID(), Status.ACTIVE);
					}
					
					if (r.getAltName() != null)
					{
						//Need to create this UUID to be different than forward name, in case forward and reverse are identical (like 'RO')
						UUID descUUID = ConverterUUID.createNamespaceUUIDFromStrings(concept.getPrimordialUuid().toString(), r.getInverseFSNName(), 
								DescriptionType.SYNONYM.name(), "false", "inverse");
						//Yes, this looks funny, no its not a copy/paste error.  We swap the FSN and alt names for... it a long story.  42.
						eConcepts_.addDescription(concept, descUUID, r.getInverseFSNName(), DescriptionType.SYNONYM, false, 
								ptDescriptions_.get(sab).getProperty("Inverse Synonym").getUUID(),
								ptDescriptions_.get(sab).getProperty("Inverse Synonym").getPropertyType().getPropertyTypeReferenceSetUUID(), Status.ACTIVE);
					}
					
					if (r.getInverseDescription() != null)
					{
						eConcepts_.addDescription(concept, r.getInverseDescription(), DescriptionType.DEFINITION, true, 
								ptDescriptions_.get(sab).getProperty("Inverse Description").getUUID(),
								ptDescriptions_.get(sab).getProperty("Inverse Description").getPropertyType().getPropertyTypeReferenceSetUUID(), Status.ACTIVE);
					}
					
					if (r.getRelType() != null)
					{
						Relationship generalRel = nameToRel_.get(r.getRelType());
						
						eConcepts_.addUuidAnnotation(concept, (mapToIsa.containsKey(generalRel.getFSNName()) ? relationships.getProperty(generalRel.getFSNName()) : 
							associations.getProperty(generalRel.getFSNName())).getUUID(), ptRelationshipMetadata_.getProperty("General Rel Type").getUUID());
					}
					
					if (r.getInverseRelType() != null)
					{
						Relationship generalRel = nameToRel_.get(r.getInverseRelType());
						
						eConcepts_.addUuidAnnotation(concept, (mapToIsa.containsKey(generalRel.getFSNName()) ? relationships.getProperty(generalRel.getFSNName()) : 
							associations.getProperty(generalRel.getFSNName())).getUUID(), 
								ptRelationshipMetadata_.getProperty("Inverse General Rel Type").getUUID());
					}
					
					for (String sctCode : r.getRelSnomedCode())
					{
						eConcepts_.addUuidAnnotation(concept, UuidT3Generator.fromSNOMED(sctCode), 
								ptRelationshipMetadata_.getProperty("Snomed Code").getUUID());
					}
					
					for (String sctCode : r.getInverseRelSnomedCode())
					{
						eConcepts_.addUuidAnnotation(concept, UuidT3Generator.fromSNOMED(sctCode), 
								ptRelationshipMetadata_.getProperty("Inverse Snomed Code").getUUID());
					}
				}
			});
		}
		
		if (relationships.getProperties().size() > 0)
		{
			eConcepts_.loadMetaDataItems(relationships, terminologyMetadataRoot, dos_);
		}
		ptRelationships_.put(sab, relationships);
		if (associations.getProperties().size() > 0)
		{
			eConcepts_.loadMetaDataItems(associations, terminologyMetadataRoot, dos_);
		}
		ptAssociations_.put(sab, associations);
	}
	
	private void processSemanticTypes(TtkConceptChronicle concept, ResultSet rs) throws SQLException
	{
		while (rs.next())
		{
			try
			{
				TtkRefexDynamicMemberChronicle annotation = eConcepts_.addUuidAnnotation(concept, semanticTypes_.get(rs.getString("TUI")), ptUMLSAttributes_.getProperty("STY").getUUID());
				if (rs.getString("ATUI") != null)
				{
					eConcepts_.addStringAnnotation(annotation, rs.getString("ATUI"), ptUMLSAttributes_.getProperty("ATUI").getUUID(), Status.ACTIVE);
				}
	
				if (rs.getObject("CVF") != null)  //might be an int or a string
				{
					eConcepts_.addStringAnnotation(annotation, rs.getString("CVF"), ptUMLSAttributes_.getProperty("CVF").getUUID(), Status.ACTIVE);
				}
			}
			catch (RuntimeException e)
			{
				//ok if dupe - this can happen due to multiple merges onto an existing SCT concept
				if (!e.toString().contains("duplicate UUID"))
				{
					throw e;
				}
			}
		}
		rs.close();
	}
	
	private UUID createCUIConceptUUID(String cui)
	{
		return ConverterUUID.createNamespaceUUIDFromString("CUI:" + cui, true);
	}
	
//	private UUID createCuiSabCodeConceptUUID(String cui, String sab, String code)
//	{
//		return ConverterUUID.createNamespaceUUIDFromString("CODE:" + cui + ":" + sab + ":" + code, true);
//	}
	
	/**
	 * @throws SQLException
	 * @throws PropertyVetoException 
	 */
	private int addRelationships(TtkConceptChronicle concept, List<REL> relationships) throws SQLException, PropertyVetoException
	{
		int createdCount = 0;
		//preprocess - set up the source and target UUIDs for this rel, so we can identify duplicates.
		//Note - the duplicates we are detecting here are rels that point to the same WB code concept, that occur 
		//due to the way that we combine AUI's.  This is not detecting duplicates caused by forward/reverse rels in the UMLS.
		for (REL relationship : relationships)
		{
			relationship.setSourceUUID(concept.getPrimordialUuid());
			
			if (relationship.getSourceAUI() == null)
			{
				if (allowedSCTTargets.get(relationship.getTargetCUI()) != null)
				{
					//map to existing SCT concept
					relationship.setTargetUUID(sctIDToUUID_.get(allowedSCTTargets.get(relationship.getTargetCUI())));
				}
				else
				{
					//must be an RxNorm concept we are creating
					relationship.setTargetUUID(createCUIConceptUUID(relationship.getTargetCUI()));	
				}
				
			}
			else
			{
				throw new RuntimeException("don't yet handle AUI associations");
//				relationship.setTargetUUID(createCuiSabCodeConceptUUID(relationship.getRxNormTargetCUI(), 
//						relationship.getTargetSAB(), relationship.getTargetCode()));
			}

			//We currently don't check the properties on the (duplicate) inverse rels to make sure they are all present - we assume that they 
			//created the inverse relationships as an exact copy of the primary rel direction.  So, just checking the first rel from our dupe list is good enough
			if (isRelPrimary(relationship.getRel(), relationship.getRela()))
			{
				//This can happen when the reverse of the rel equals the rel... sib/sib
				if (relCheckIsRelLoaded(relationship))
				{
					continue;
				}
				
				Property relTypeAsRel = ptRelationships_.get(relationship.getSab()).getProperty(
						(relationship.getRela() == null ? relationship.getRel() : relationship.getRela()));
				
				PropertyAssociation relTypeAsAssn = (PropertyAssociation)ptAssociations_.get(relationship.getSab()).getProperty(
						(relationship.getRela() == null ? relationship.getRel() : relationship.getRela()));
				
				TtkComponentChronicle<?, ?> r;
				if (relTypeAsRel != null && (relTypeAsAssn == null || (relTypeAsAssn != null && handleAsRel(relationship))))
				{
					if (relTypeAsRel.getWBTypeUUID() == null)
					{
						r = eConcepts_.addRelationship(concept, (relationship.getRui() != null ? 
								ConverterUUID.createNamespaceUUIDFromString("RUI:" + relationship.getRui()) : null),
								relationship.getTargetUUID(), relTypeAsRel.getUUID(), null, null, null);
					}
					else  //need to swap out to the wb rel type (usually, isa)
					{
						r = eConcepts_.addRelationship(concept, (relationship.getRui() != null ? 
								ConverterUUID.createNamespaceUUIDFromString("RUI:" + relationship.getRui()) : null),
								relationship.getTargetUUID(), relTypeAsRel.getWBTypeUUID(), relTypeAsRel.getUUID(), 
								relTypeAsRel.getPropertyType().getPropertyTypeReferenceSetUUID(), null);
					}
				}
				else if (relTypeAsAssn != null)
				{
					r = eConcepts_.addAssociation(concept.getConceptAttributes(), (relationship.getRui() != null ? 
							ConverterUUID.createNamespaceUUIDFromString("RUI:" + relationship.getRui()) : null),
							relationship.getTargetUUID(), relTypeAsAssn.getUUID(), Status.ACTIVE, null);
				}
				else
				{
					throw new RuntimeException("Unexpected rel handling");
				}
				createdCount++;

				//disabled debug code
				//conceptUUIDsUsedInRels_.add(concept.getPrimordialUuid());
				//conceptUUIDsUsedInRels_.add(relationship.getTargetUUID());

				//Add the annotations
				HashSet<String> addedRUIs = new HashSet<>();
				if (relationship.getRela() != null)  //we already used rela - annotate with rel.
				{
					Property genericType = ptAssociations_.get(relationship.getSab()).getProperty(relationship.getRel()) == null ? 
							ptRelationships_.get(relationship.getSab()).getProperty(relationship.getRel()) :
								ptAssociations_.get(relationship.getSab()).getProperty(relationship.getRel());
					boolean reversed = false;
					if (genericType == null && relationship.getRela().equals("mapped_from"))
					{
						//This is to handle non-sensical data in UMLS... they have no consistency in the generic rel they assign - sometimes RB, sometimes RN.
						//reverse it - currently, only an issue on 'mapped_from' rels - as the code in Relationship.java has some exceptions for this type.
						genericType = ptAssociations_.get(relationship.getSab()).getProperty(reverseRel(relationship.getRel())) == null ? 
								ptRelationships_.get(relationship.getSab()).getProperty(reverseRel(relationship.getRel())) :
									ptAssociations_.get(relationship.getSab()).getProperty(reverseRel(relationship.getRel()));
						reversed = true;
					}
					eConcepts_.addUuidAnnotation(r, genericType.getUUID(), 
							ptUMLSAttributes_.getProperty(reversed ? "Generic rel type (inverse)" : "Generic rel type").getUUID());
				}
				if (relationship.getRui() != null)
				{
					if (!addedRUIs.contains(relationship.getRui()))
					{
						eConcepts_.addStringAnnotation(r, relationship.getRui(), ptUMLSAttributes_.getProperty("RUI").getUUID(), Status.ACTIVE);
						addedRUIs.add(relationship.getRui());
						satRelStatement_.clearParameters();
						satRelStatement_.setString(1, relationship.getRui());
						ResultSet nestedRels = satRelStatement_.executeQuery();
						processSAT(r, nestedRels, null, relationship.getSab(), null);
					}
				}
				if (relationship.getRg() != null)
				{
					eConcepts_.addStringAnnotation(r, relationship.getRg(), ptUMLSAttributes_.getProperty("RG").getUUID(), Status.ACTIVE);
				}
				if (relationship.getDir() != null)
				{
					eConcepts_.addStringAnnotation(r, relationship.getDir(), ptUMLSAttributes_.getProperty("DIR").getUUID(), Status.ACTIVE);
				}
				if (relationship.getSuppress() != null)
				{
					eConcepts_.addUuidAnnotation(r, ptSuppress_.getProperty(relationship.getSuppress()).getUUID(), 
							ptUMLSAttributes_.getProperty("SUPPRESS").getUUID());
				}

				if (relationship.getCvf() != null)
				{
					if (relationship.getCvf().equals("4096"))
					{
						eConcepts_.addDynamicRefsetMember(cpcRefsetConcept_, r.getPrimordialComponentUuid(), null, Status.ACTIVE, null);
					}
					else
					{
						throw new RuntimeException("Unexpected value in RXNSAT cvf column '" + relationship.getCvf() + "'");
					}
				}
				
				relCheckLoadedRel(relationship);
			}
			else
			{
				relCheckSkippedRel(relationship);
			}
		}
		return createdCount;
	}
	
	
	private boolean handleAsRel(REL relationship) throws SQLException
	{
		/**
		 * Rule 1 is about:
		 * Convert RxNorm Tradename_of to is_a between RxNorm SBD and RxNorm SCD
		 * tradename_of is created as both an association and a relationship - in the rel form, it maps to is_a.
		 */
		if (hasTTYType(relationship.getSourceCUI(), "SBD") && hasTTYType(relationship.getTargetCUI(), "SCD"))
		{
			convertedTradenameCount_.incrementAndGet();
			return true;
		}

		return false;
	}
	
	private boolean hasTTYType(String cui, String tty) throws SQLException
	{
		hasTTYType_.setString(1, cui);
		hasTTYType_.setString(2, tty);
		ResultSet rs = hasTTYType_.executeQuery();
		if (rs.next())
		{
			return rs.getInt("count") > 0;
		}
		throw new RuntimeException("Unexpected");
	}

	private boolean isRelPrimary(String relName, String relaName)
	{
		if (relaName != null)
		{
			return nameToRel_.get(relaName).getFSNName().equals(relaName);
		}
		else
		{
			return nameToRel_.get(relName).getFSNName().equals(relName);
		}
	}
	
	private String reverseRel(String eitherRelType)
	{
		if (eitherRelType == null)
		{
			return null;
		}
		Relationship r = nameToRel_.get(eitherRelType);
		if (r.getFSNName().equals(eitherRelType))
		{
			return r.getInverseFSNName();
		}
		else if (r.getInverseFSNName().equals(eitherRelType))
		{
			return r.getFSNName();
		}
		else
		{
			throw new RuntimeException("gak");
		}
	}
	
	private void relCheckLoadedRel(REL rel)
	{
		loadedRels_.add(rel.getRelHash());
		skippedRels_.remove(rel.getRelHash());
	}
	
	private boolean relCheckIsRelLoaded(REL rel)
	{
		return loadedRels_.contains(rel.getRelHash());
	}
	
	/**
	 * Call this when a rel wasn't added because the rel was listed with the inverse name, rather than the primary name. 
	 */
	private void relCheckSkippedRel(REL rel)
	{
		skippedRels_.add(rel.getInverseRelHash(string -> nameToRel_.get(string)));
	}
	
	private void checkRelationships()
	{
		//if the inverse relationships all worked properly, skipped should be empty when loaded is subtracted from it.
		for (UUID uuid : loadedRels_)
		{
			skippedRels_.remove(uuid);
		}
		
		if (skippedRels_.size() > 0)
		{
			ConsoleUtil.printErrorln("Relationship design error - " +  skippedRels_.size() + " were skipped that should have been loaded");
		}
		else
		{
			ConsoleUtil.println("Yea! - no missing relationships!");
		}
	}
	
	public static void main(String[] args) throws MojoExecutionException
	{
		RxNormMojo mojo = new RxNormMojo();
		mojo.outputDirectory = new File("../RxNorm-econcept/target");
		mojo.inputFileLocation = new File("../RxNorm-econcept/target/generated-resources/src/");
		mojo.execute();
	}
}
