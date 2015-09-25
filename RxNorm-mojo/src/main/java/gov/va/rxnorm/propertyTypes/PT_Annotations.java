package gov.va.rxnorm.propertyTypes;

import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Annotations;
import gov.vha.isaac.ochre.api.component.sememe.version.dynamicSememe.DynamicSememeColumnInfo;
import gov.vha.isaac.ochre.api.component.sememe.version.dynamicSememe.DynamicSememeDataType;
import gov.vha.isaac.ochre.model.constants.IsaacMetadataConstants;

/**
 * Properties from the DTS ndf load which are treated as alternate IDs within the workbench.
 * @author Daniel Armbrust
 */
public class PT_Annotations extends BPT_Annotations
{
	public PT_Annotations()
	{
		indexByAltNames();
		addProperty("Unique identifier for atom ", null, "RXAUI", "(RxNorm Atom Id)");
		addProperty("Source asserted atom identifier", null, "SAUI", null);
		addProperty("Source asserted concept identifier", null, "SCUI", null);
		addProperty("Source Vocabulary", null, "SAB", null, false, -1, 
				new DynamicSememeColumnInfo[] { new DynamicSememeColumnInfo(null, 0, IsaacMetadataConstants.DYNAMIC_SEMEME_COLUMN_VALUE.getUUID(),
				DynamicSememeDataType.UUID, null, true, null, null)});
		addProperty("Code", null, "CODE", "\"Most useful\" source asserted identifier (if the source vocabulary has more than one identifier)" 
				+ ", or a RxNorm-generated source entry identifier (if the source vocabulary has none.)");
		addProperty("Suppress", null, "SUPPRESS", null, false, -1, 
				new DynamicSememeColumnInfo[] { new DynamicSememeColumnInfo(null, 0, IsaacMetadataConstants.DYNAMIC_SEMEME_COLUMN_VALUE.getUUID(),
				DynamicSememeDataType.UUID, null, true, null, null)});
		addProperty("Term Type Class", null, "tty_class", null);
		addProperty("STYPE", null, "The name of the column in RXNCONSO.RRF or RXNREL.RRF that contains the identifier to which the attribute is attached, e.g., CUI, AUI.");
		addProperty("STYPE1", null, "The name of the column in RXNCONSO.RRF that contains the identifier used for the first concept or first atom in source of the relationship (e.g., 'AUI' or 'CUI')");
		addProperty("STYPE2", null, "The name of the column in RXNCONSO.RRF that contains the identifier used for the second concept or second atom in the source of the relationship (e.g., 'AUI' or 'CUI')");
		addProperty("Source Asserted Attribute Identifier", null, "SATUI", "Source asserted attribute identifier (optional - present if it exists)");
		addProperty("Semantic Type tree number", null, "STN", null);
		addProperty("Semantic Type", null, "STY", null, false, -1, 
				new DynamicSememeColumnInfo[] { new DynamicSememeColumnInfo(null, 0, IsaacMetadataConstants.DYNAMIC_SEMEME_COLUMN_VALUE.getUUID(),
				DynamicSememeDataType.UUID, null, true, null, null)});
		addProperty("Content View Flag", null, "CVF", "Bit field used to flag rows included in Content View.");//note - this is undocumented in RxNorm - used on the STY table - description_ comes from UMLS
		addProperty("URI");
		addProperty("RG", null, "Machine generated and unverified indicator");
		addProperty("Generic rel type", null, null, "Generic rel type for this relationship", false, -1, 
				new DynamicSememeColumnInfo[] { new DynamicSememeColumnInfo(null, 0, IsaacMetadataConstants.DYNAMIC_SEMEME_COLUMN_VALUE.getUUID(),
				DynamicSememeDataType.UUID, null, true, null, null)});		
		
		//Things that used to be IDs, below this point
		addProperty("RXCUI", "RxNorm Concept ID", "RxNorm Unique identifier for concept");
		addProperty("TUI", "RxNorm Semantic Type ID", "Unique identifier of Semantic Type");
		addProperty("RUI", "RxNorm Relationship ID", "Unique identifier for Relationship");
		addProperty("ATUI", "RxNorm Attribute ID", "Unique identifier for attribute");
	}
}
