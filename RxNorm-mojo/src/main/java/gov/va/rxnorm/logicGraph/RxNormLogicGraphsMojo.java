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
package gov.va.rxnorm.logicGraph;

import static gov.vha.isaac.ochre.api.logic.LogicalExpressionBuilder.And;
import static gov.vha.isaac.ochre.api.logic.LogicalExpressionBuilder.ConceptAssertion;
import static gov.vha.isaac.ochre.api.logic.LogicalExpressionBuilder.Feature;
import static gov.vha.isaac.ochre.api.logic.LogicalExpressionBuilder.FloatLiteral;
import static gov.vha.isaac.ochre.api.logic.LogicalExpressionBuilder.NecessarySet;
import static gov.vha.isaac.ochre.api.logic.LogicalExpressionBuilder.SomeRole;
import gov.vha.isaac.metadata.coordinates.LogicCoordinates;
import gov.vha.isaac.metadata.source.IsaacMetadataAuxiliaryBinding;
import gov.vha.isaac.mojo.external.QuasiMojo;
import gov.vha.isaac.ochre.api.DataTarget;
import gov.vha.isaac.ochre.api.Get;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.Util;
import gov.vha.isaac.ochre.api.chronicle.LatestVersion;
import gov.vha.isaac.ochre.api.commit.ChangeCheckerMode;
import gov.vha.isaac.ochre.api.component.concept.ConceptChronology;
import gov.vha.isaac.ochre.api.component.sememe.SememeChronology;
import gov.vha.isaac.ochre.api.component.sememe.version.DynamicSememe;
import gov.vha.isaac.ochre.api.component.sememe.version.LogicGraphSememe;
import gov.vha.isaac.ochre.api.component.sememe.version.MutableLogicGraphSememe;
import gov.vha.isaac.ochre.api.coordinate.EditCoordinate;
import gov.vha.isaac.ochre.api.index.IndexServiceBI;
import gov.vha.isaac.ochre.api.index.SearchResult;
import gov.vha.isaac.ochre.api.logic.LogicalExpression;
import gov.vha.isaac.ochre.api.logic.LogicalExpressionBuilder;
import gov.vha.isaac.ochre.api.logic.LogicalExpressionBuilderService;
import gov.vha.isaac.ochre.api.logic.Node;
import gov.vha.isaac.ochre.api.logic.NodeSemantic;
import gov.vha.isaac.ochre.api.logic.assertions.Assertion;
import gov.vha.isaac.ochre.api.task.TimedTask;
import gov.vha.isaac.ochre.model.logic.LogicalExpressionOchreImpl;
import gov.vha.isaac.ochre.model.logic.node.AndNode;
import gov.vha.isaac.ochre.model.logic.node.LiteralNodeFloat;
import gov.vha.isaac.ochre.model.logic.node.internal.ConceptNodeWithSequences;
import gov.vha.isaac.ochre.model.logic.node.internal.FeatureNodeWithSequences;
import gov.vha.isaac.ochre.model.logic.node.internal.RoleNodeSomeWithSequences;
import gov.vha.isaac.ochre.util.WorkExecutors;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import javafx.util.Pair;
import org.apache.maven.plugin.MojoExecutionException;
import org.jvnet.hk2.annotations.Service;


/**
 * Code to process the logic graphs into the ISAAC DB - things that can't be loaded via eConcept.
 * {@link RxNormLogicGraphsMojo}
 *
 * @author <a href="mailto:daniel.armbrust.list@gmail.com">Dan Armbrust</a>
 */
@Service(name = "load-rxnorm-logic-graphs")
public class RxNormLogicGraphsMojo extends QuasiMojo
{

	@Override
	public void execute() throws MojoExecutionException
	{
		getLog().info("RxNorm Logic Graph Processing Begins " + new Date().toString());
		
		TimedTask<Void> task = new Worker();
		LookupService.getService(WorkExecutors.class).getExecutor().submit(task);
		try
		{
			Util.addToTaskSetAndWaitTillDone(task);
		}
		catch (InterruptedException | ExecutionException e)
		{
			throw new MojoExecutionException("Failure", e);
		}

		getLog().info("LOINC Tech Preview Processing Ends " + new Date().toString());
		
	}
	
	private class Worker extends TimedTask<Void>
	{
		@SuppressWarnings("deprecation")
		@Override
		protected Void call() throws Exception
		{
			getLog().info("Processing RxNorm Concrete Domains");
			updateTitle("Processing RxNorm Concrete Domains");
			updateMessage("Building Logic Graphs");
			updateProgress(1, 3);
			
			EditCoordinate ec = Get.configurationService().getDefaultEditCoordinate();
			ConceptChronology<?> unitConcept = Get.conceptService().getConcept(UUID.fromString("17055d89-84e3-3e12-9fb1-1bc4c75a122d"));  //Units (attribute)
			
			LogicalExpressionBuilderService expressionBuilderService = LookupService.getService(LogicalExpressionBuilderService.class);
			
			//Need to gather per concept, as some concepts have multiple instances of this assemblage
			
			HashMap<Integer, ArrayList<String>> entries = new HashMap<Integer, ArrayList<String>>();  //con nid to values
			Get.sememeService().getSememesFromAssemblage(findAssemblageNid("RXN_AVAILABLE_STRENGTH")).forEach(sememe ->
			{
				try
				{
					@SuppressWarnings({ "rawtypes", "unchecked" })
					Optional<LatestVersion<DynamicSememe>> ds = ((SememeChronology)sememe).getLatestVersion(DynamicSememe.class, Get.configurationService().getDefaultStampCoordinate());
					if (ds.isPresent())
					{
						@SuppressWarnings("rawtypes")
						DynamicSememe dsv = ds.get().value();
						int descriptionSememe = dsv.getReferencedComponentNid();
						int conceptNid = Get.sememeService().getSememe(descriptionSememe).getReferencedComponentNid();
						String value = dsv.getData()[0].getDataObject().toString();
						String[] multipart = value.split(" / ");
						
						ArrayList<String> itemEntries = entries.get(conceptNid);
						if (itemEntries == null)
						{
							itemEntries = new ArrayList<>();
							entries.put(conceptNid, itemEntries);
						}
						for (String s : multipart)
						{
							itemEntries.add(s);
						}
					}
				}	
				catch (Exception e)
				{
					getLog().error("Failed reading " + sememe, e);
				}
				
			});
						
			for (Entry<Integer, ArrayList<String>> item : entries.entrySet())
			{
				try
				{
						
					Optional<LatestVersion<? extends LogicalExpression>> existingLogicExpr = Get.logicService().getLogicalExpression(item.getKey(), 
							LogicCoordinates.getStandardElProfile().getStatedAssemblageSequence(), 
							Get.configurationService().getDefaultStampCoordinate());
					
					LogicalExpression existing = null;
					if (existingLogicExpr.isPresent())
					{
						existing = existingLogicExpr.get().value();
					}
					
					LogicalExpressionBuilder leb = expressionBuilderService.getLogicalExpressionBuilder();
					ArrayList<Assertion> assertions = new ArrayList<>();
					
					for (String part : item.getValue())
					{
						if (part.length() > 0)
						{
							Pair<Float, UNIT> parsed = parseSpecifics(part);
							
							//If we could use the builder (but we can't) it might look sort of like this:
							//Just values
//								NecessarySet(And(
//										Feature(IsaacMetadataAuxiliaryBinding.HAS_STRENGTH, FloatLiteral(parsedNumerator.getKey(), leb))));
							
							//values and units
//								NecessarySet(And(
//										SomeRole(IsaacMetadataAuxiliaryBinding.ROLE_GROUP, And(
//												Feature(IsaacMetadataAuxiliaryBinding.HAS_STRENGTH, FloatLiteral(parsed.getKey(), leb),
//												SomeRole(unitConcept, ConceptAssertion(MLConceptFromParsedValue)))))));
							
							if (existing == null)
							{
								assertions.add(SomeRole(IsaacMetadataAuxiliaryBinding.ROLE_GROUP, And(
											Feature(IsaacMetadataAuxiliaryBinding.HAS_STRENGTH, FloatLiteral(parsed.getKey(), leb)),
											SomeRole(unitConcept, 
													ConceptAssertion(Get.conceptService().getConcept(parsed.getValue().getConceptUUID()), leb)))));
							}
							else
							{
								//We can't use the builder, because there is currently no way to combine the logic graph from this builder, with 
								//the existing logic graph.  So, instead, manually build these nodes into the preexisting logic graph, below.
								
								boolean found = false;
								for (Node n : existing.getRoot().getChildren())
								{
									if (n.getNodeSemantic() == NodeSemantic.NECESSARY_SET)
									{
										if (n.getChildren().length == 1 && n.getChildren()[0].getNodeSemantic() == NodeSemantic.AND)
										{
											FeatureNodeWithSequences feature = new FeatureNodeWithSequences(
													(LogicalExpressionOchreImpl)existing, 
													IsaacMetadataAuxiliaryBinding.HAS_STRENGTH.getConceptSequence(), 
													new LiteralNodeFloat((LogicalExpressionOchreImpl)existing, parsed.getKey().floatValue()));
											
											RoleNodeSomeWithSequences unitRole = new RoleNodeSomeWithSequences((LogicalExpressionOchreImpl)existing, 
													unitConcept.getConceptSequence(), 
													new ConceptNodeWithSequences((LogicalExpressionOchreImpl)existing, 
															Get.identifierService().getConceptSequenceForUuids(parsed.getValue().getConceptUUID())));
											
											AndNode andNode = new AndNode((LogicalExpressionOchreImpl)existing, feature, unitRole);
											
											RoleNodeSomeWithSequences groupingRole = new RoleNodeSomeWithSequences((LogicalExpressionOchreImpl)existing, 
													IsaacMetadataAuxiliaryBinding.ROLE_GROUP.getConceptSequence(), andNode);
											
											n.getChildren()[0].addChildren(groupingRole);
											found = true;
											break;
										}
									}
								}
								
								if (!found)
								{
									throw new RuntimeException("oops! - couldn't merge on necessary");
								}
							}
						}
					}
					
					if (existing != null)
					{
						//I should find one and only 1, as we read it above, from the logic expression service, and it validates.
						SememeChronology<?> sc = Get.sememeService().getSememesForComponentFromAssemblage(item.getKey(), 
								LogicCoordinates.getStandardElProfile().getStatedAssemblageSequence()).findFirst().get();
						
						@SuppressWarnings("unchecked")
						MutableLogicGraphSememe<?> mls = ((SememeChronology<LogicGraphSememe<?>>)sc).createMutableVersion(MutableLogicGraphSememe.class, 
								gov.vha.isaac.ochre.api.State.ACTIVE, 
								ec); 
						
						mls.setGraphData(existing.getData(DataTarget.INTERNAL));
						
						Get.commitService().addUncommitted(sc);
					}
					else
					{
						NecessarySet(And(assertions.toArray(new Assertion[0])));
						LogicalExpression le = leb.build();
						Get.sememeBuilderService().getLogicalExpressionSememeBuilder(le, item.getKey(), 
								LogicCoordinates.getStandardElProfile().getStatedAssemblageSequence()).build(ec, ChangeCheckerMode.ACTIVE);
					}
					
				}
				catch (Exception e)
				{
					getLog().error("Failed creating logic graph for concept id  " + item.getKey(), e);
				}
			}
			
			getLog().info("Committing");
			updateMessage("Committing");
			updateProgress(2, 3);
			
			Get.commitService().commit("Adding RxNorm Concrete Domains").get();
			
			getLog().info("Done");
			updateMessage("Done");
			updateProgress(3, 3);
			
			return null;
		}
		
	}
	
	private Pair<Float, UNIT> parseSpecifics(String value)
	{
		value = removeParenStuff(value).trim();
		String[] parts = value.split(" ");
		if (parts.length == 1)
		{
			return new Pair<>(1.0f, UNIT.parse(parts[0]));
		}
		else if (parts.length == 2)
		{
			return new Pair<>(Float.parseFloat(parts[0]), UNIT.parse(parts[1]));
		}
		throw new RuntimeException("Wrong number of parts in '" + value + "'");
	}
	
	private String removeParenStuff(String input)
	{
		if (input.contains("(") && input.contains(")"))
		{
			int i = input.indexOf("(");
			int z = input.lastIndexOf(")");
			if (z < i)
			{
				throw new RuntimeException("oops");
			}
			return input.substring(0, i) + input.substring(z + 1, input.length());
		}
		return input;
	}
	
	private int findAssemblageNid(String uniqueName)
	{
		IndexServiceBI si = LookupService.get().getService(IndexServiceBI.class, "description indexer");
		if (si != null)
		{
			//force the prefix algorithm, and add a trailing space - quickest way to do an exact-match type of search
			List<SearchResult> result = si.query(uniqueName + " ", true, 
					null, 5, Long.MIN_VALUE);
			if (result.size() > 0)
			{
				return Get.sememeService().getSememe(result.get(0).getNid()).getReferencedComponentNid();
			}
		}
		throw new RuntimeException("Can't find assemblage nid with the name " + uniqueName);
	}
}
