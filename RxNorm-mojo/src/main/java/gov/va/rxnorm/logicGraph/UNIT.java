package gov.va.rxnorm.logicGraph;

import gov.vha.isaac.ochre.util.UuidT5Generator;
import java.util.UUID;

public enum UNIT
{
	//Everything but Actuation is from snomed
	MG("MilliGrams"), 
	ML("MilliLiter"),
	PERCENT("Percent", "%"),
	UNT("Units"), 
	ACTUAT("Actuation"), 
	HR("Hour"), 
	MEQ("MilliEquivalent"),
	CELLS("Cells"),
	BAU("Bioequivalent Allergy Units"),
	MGML("MilliGrams per MilliLiter", "MG/ML"),
	UNTML("Units per MilliLiter", "UNT/ML"),
	MGMG("Milligrams per Milligram", "MG/MG"),
	MEQML("MilliEquivalent per Milliliter", "MEQ/ML"),
	BAUML("Bioequivalent Allergy Units per MilliLiter", "BAU/ML"),
	AU("Arbitrary Unit"),   //not sure about this
	SQCM("Square Centimeter"),  //probably
	MCI("millicurie"),   //not sure about this
	PNU("Protein nitrogen unit"),
	CELLSML("Cells per MilliLiter", "CELLS/ML"),
	AUML("Arbitrary Unit per MilliLiter", "AU/ML"),
	MLML("MilliLiter per MilliLiter", "ML/ML"),
	MGHR("MilliGrams per Hour", "MG/HR"),
	MGACTUAT("MilliGrams per Actuation", "MG/ACTUAT"),
	UNTMG("Units per MilliGram", "UNT/MG"),
	MEQMG("MilliEquivalent per MilliGram", "MEQ/MG"),
	UNTACTUAT("Units per Actuation", "UNT/ACTUAT"),
	IR("Index of Reactivity", "IR"),  //maybe?
	MCIML("milicurie per MilliLiter", "MCI/ML"),  //maybe?
	MGSQCM("MilliGram per Square Centimeter", "MG/SQCM"),  //probably
	PNUML("Protein nitrogen unit per MilliLiter", "PNU/ML");

	
	private String fullName_;
	private String altName_;
	
	private UNIT(String fsn)
	{
		fullName_ = fsn;
	}
	private UNIT(String fsn, String altName)
	{
		fullName_ = fsn;
		altName_ = altName;
	}
	
	public String getFullName()
	{
		return fullName_;
	}
	
	/**
	 * Note that by chance, if this returns a type 3 UUID, then it is an existing SCT concept.
	 * If it returns type 5, we invented the UUID, and someone needs to make the concept.
	 */
	public UUID getConceptUUID()
	{
		//Note, I would normally define these in the construtor of the enum, as constants, but maven is #@%$#%#@ broken and DIES 
		//when it encounters (perfectly valid) code that it can't parse, because it is broken. 
		switch (this)
		{
			case MEQ:
				return UUID.fromString("2def410e-b419-341a-a469-4b97c3e4fe16");
			case HR:
				return UUID.fromString("aca700b1-1500-3c2f-bcc6-87f3121e7913");
			case MG:
				return UUID.fromString("89cb8d09-3a3c-31e6-94ea-05fe8ff17551");
			case ML:
				return UUID.fromString("f48333ed-4449-3a48-b7b1-7d9f0a1df0e6");
			case PERCENT:
				return UUID.fromString("31e4aab3-5b9b-39b7-89a5-52b4738e03a6");
			case UNT:
				return UUID.fromString("17055d89-84e3-3e12-9fb1-1bc4c75a122d");
				//real concepts above here (from sct)
				//concepts that need to be constructed, below here
			case ACTUAT:
			case CELLS:
			case BAU:
			case MGML:
			case UNTML:
			case AU:
			case BAUML:
			case MCI:
			case MEQML:
			case MGMG:
			case PNU:
			case SQCM:
			case AUML:
			case CELLSML:
			case MGACTUAT:
			case MGHR:
			case MLML:
			case UNTMG:
			case MEQMG:
			case IR:
			case MCIML:
			case MGSQCM:
			case PNUML:
			case UNTACTUAT:
				return UuidT5Generator.get(this.name());
			default :
				throw new RuntimeException("oops");
		}
	}
	
	public boolean hasRealSCTConcept()
	{
		if (getConceptUUID().toString().charAt(14) == '3')
		{
			return true;
		}
		else if (getConceptUUID().toString().charAt(14) == '5')
		{
			return false;
		}
		else
		{
			throw new RuntimeException("oops");
		}
	}
	
	
	public static UNIT parse(String value)
	{
		for (UNIT u : UNIT.values())
		{
			if (u.name().equals(value.trim()) || value.trim().equals(u.altName_))
			{
				return u;
			}
		}
		throw new RuntimeException("Can't match " + value);
	}
}
