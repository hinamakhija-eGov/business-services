package org.egov.demand.repository.querybuilder;

import java.util.List;

import org.egov.demand.amendment.model.AmendmentCriteria;
import org.egov.demand.util.Util;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

@Component
public class AmendmentQueryBuilder {
	
	@Autowired
	private Util util;
	
	public static final String AMENDMENT_UPDATE_QUERY = "UPDATE egbs_amendment SET status=?, amendeddemandid=?, lastmodifiedby=?,"
			+ " lastmodifiedtime=?, additionaldetails=? WHERE tenantid=? AND amendmentid=?;";
	
	public static final String AMENDMENT_INSERT_QUERY = "INSERT INTO egbs_amendment (id, tenantid, amendmentid, businessservice,"
			+ " consumercode, amendmentreason, reasondocumentnumber, status, effectivetill, effectivefrom,"
			+ " amendeddemandid, createdby, createdtime, lastmodifiedby, lastmodifiedtime, additionaldetails) "
			+ "	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

	public static final String AMENDMENT_TAXDETAIL_INSERT_QUERY = "INSERT INTO egbs_amendment_taxdetail(id, amendmentid, taxheadcode, taxamount)"
			+ " VALUES (?, ?, ?, ?);";

	public static final String DOCUMET_INSERT_QUERY = "INSERT INTO egbs_document(id, amendmentid, documenttype, filestoreid, documentuid, status)"
			+ " VALUES (?, ?, ?, ?, ?, ?);";
	
	public static final String AMENDMENT_SEARCH_QUERY = "SELECT amendment.id as amendmentuuid, tenantid, amendment.amendmentid as amendmentid, businessservice,"
			+ " consumercode, amendmentreason, reasondocumentnumber, amendment.status as amendmentstatus, effectivetill,"
			+ " effectivefrom, amendeddemandid, createdby, createdtime, lastmodifiedby, lastmodifiedtime, additionaldetails,"
			+ " amdl.id as detailid, amdl.amendmentid as detailamendmentid, taxheadcode, taxamount, doc.id as docid,"
			+ " doc.amendmentid as docamendmentid, documentType, fileStoreid, documentuid, doc.status as docstatus "
			+ " FROM egbs_amendment amendment "
			+ " INNER JOIN "
			+ " egbs_amendment_taxdetail amdl ON amendment.id = amdl.amendmentid " 
			+ "	INNER JOIN egbs_document doc ON amendment.id = doc.amendmentid WHERE ";

	
	public String getSearchQuery(AmendmentCriteria amendmentCriteria, List<Object> preparedStatementValues) {

		StringBuilder queryBuilder = new StringBuilder(AMENDMENT_SEARCH_QUERY);

		queryBuilder.append(" amendment.tenantid = ? ");
		preparedStatementValues.add(amendmentCriteria.getTenantId());
		
		if (!CollectionUtils.isEmpty(amendmentCriteria.getConsumerCode())) {

			addAndClause(queryBuilder);
			queryBuilder.append(" consumercode IN "
					+ util.getIdsQueryForList(amendmentCriteria.getConsumerCode(), preparedStatementValues));
		}
		
		if (amendmentCriteria.getBusinessService() != null) {

			addAndClause(queryBuilder);
			queryBuilder.append(" businessservice=? ");
			preparedStatementValues.add(amendmentCriteria.getBusinessService());
		}
		
		if (amendmentCriteria.getAmendmentId() != null) {

			addAndClause(queryBuilder);
			queryBuilder.append(" amendment.amendmentid=? ");
			preparedStatementValues.add(amendmentCriteria.getAmendmentId());
		}
		
		if (amendmentCriteria.getStatus() != null) {

			addAndClause(queryBuilder);
			queryBuilder.append(" amendment.status=? ");
			preparedStatementValues.add(amendmentCriteria.getStatus().toString());
		}
		
		addPagingClause(queryBuilder, preparedStatementValues);
		return queryBuilder.toString();
	}

	private static void addPagingClause(StringBuilder amendmentQueryBuilder, List<Object> preparedStatementValues) {
		
		amendmentQueryBuilder.append(" LIMIT ? ");
		preparedStatementValues.add(500);
		amendmentQueryBuilder.append(" OFFSET ? ");
		preparedStatementValues.add(0);
	}
	
	private static boolean addAndClause(StringBuilder queryString) {
		queryString.append(" AND ");
		return true;
	}
}
