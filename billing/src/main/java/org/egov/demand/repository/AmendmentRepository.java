package org.egov.demand.repository;

import static org.egov.demand.repository.querybuilder.AmendmentQueryBuilder.AMENDMENT_INSERT_QUERY;
import static org.egov.demand.repository.querybuilder.AmendmentQueryBuilder.AMENDMENT_TAXDETAIL_INSERT_QUERY;
import static org.egov.demand.repository.querybuilder.AmendmentQueryBuilder.AMENDMENT_UPDATE_QUERY;
import static org.egov.demand.repository.querybuilder.AmendmentQueryBuilder.DOCUMET_INSERT_QUERY;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.egov.demand.amendment.model.Amendment;
import org.egov.demand.amendment.model.AmendmentCriteria;
import org.egov.demand.amendment.model.AmendmentRequest;
import org.egov.demand.amendment.model.AmendmentUpdate;
import org.egov.demand.amendment.model.Document;
import org.egov.demand.model.AuditDetails;
import org.egov.demand.model.DemandDetail;
import org.egov.demand.repository.querybuilder.AmendmentQueryBuilder;
import org.egov.demand.repository.rowmapper.AmendmentRowMapper;
import org.egov.demand.util.Util;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class AmendmentRepository {

	@Autowired
	private Util util;
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private AmendmentRowMapper amendmentRowMapper;
	
	@Autowired
	private AmendmentQueryBuilder amendmentQueryBuilder;
	
	@Transactional
	public void saveAmendment (AmendmentRequest amendmentRequest) {
		
		Amendment amendment = amendmentRequest.getAmendment();

		AuditDetails auditDetails = amendment.getAuditDetails();
		jdbcTemplate.update(AMENDMENT_INSERT_QUERY, new PreparedStatementSetter() {

			@Override
			public void setValues(PreparedStatement ps) throws SQLException {

				ps.setString(1, amendment.getId());
				ps.setString(2, amendment.getTenantId());
				ps.setString(3, amendment.getAmendmentId());
				ps.setString(4, amendment.getBusinessService());
				ps.setString(5, amendment.getConsumerCode());
				ps.setString(6, amendment.getAmendmentReason().toString());
				ps.setString(7, amendment.getReasonDocumentNumber());
				ps.setString(8, amendment.getStatus().toString());
				ps.setObject(9, amendment.getEffectiveTill());
				ps.setObject(10, amendment.getEffectiveFrom());
				ps.setString(11, amendment.getAmendedDemandId());
				ps.setString(12, auditDetails.getCreatedBy());
				ps.setLong  (13, auditDetails.getCreatedTime());
				ps.setString(14, auditDetails.getLastModifiedBy());
				ps.setLong  (15, auditDetails.getLastModifiedTime());
				ps.setObject(16, util.getPGObject(amendment.getAdditionalDetails()));

			}
		});
		
		saveTaxDetail(amendment.getDemandDetails(), amendment.getId());
		savedocs(amendment.getDocuments(), amendment.getId());
		
		// save document
	}
	
	private void saveTaxDetail(List<DemandDetail> demandDetails, String amendmentId) {

		jdbcTemplate.batchUpdate(AMENDMENT_TAXDETAIL_INSERT_QUERY, new BatchPreparedStatementSetter() {
			
			@Override
			public void setValues(PreparedStatement ps, int rowNum) throws SQLException {

				DemandDetail detail = demandDetails.get(rowNum);
				ps.setString(1, detail.getId());
				ps.setString(2, amendmentId);
				ps.setString(3, detail.getTaxHeadMasterCode());
				ps.setBigDecimal(4, detail.getTaxAmount());
			}
			
			@Override
			public int getBatchSize() {
				return demandDetails.size();
			}
		});
	}
	
	private void savedocs(List<Document> documents, String amendmentId) {
		
		jdbcTemplate.batchUpdate(DOCUMET_INSERT_QUERY, new BatchPreparedStatementSetter() {
			
			@Override
			public void setValues(PreparedStatement ps, int rowNum) throws SQLException {

				Document doc = documents.get(rowNum);
				ps.setString(1, doc.getId());
				ps.setString(2, amendmentId); // amendmentId as demandId
				ps.setString(3, doc.getDocumentType());
				ps.setString(4, doc.getFileStoreId());
				ps.setString(5, doc.getDocumentUid());
				ps.setString(6, "ACTIVE"); // hard-coded to active since no change possible
				
			}
			
			@Override
			public int getBatchSize() {
				return documents.size();
			}
		});
	}

	public List<Amendment> getAmendments (AmendmentCriteria amendmentCriteria) {

		List<Object> psValues = new ArrayList<>();
		String searchQuery = amendmentQueryBuilder.getSearchQuery(amendmentCriteria, psValues);
		return jdbcTemplate.query(searchQuery, psValues.toArray(), amendmentRowMapper);
	}

	@Transactional
	public void updateAmendment(List<AmendmentUpdate> amendmentUpdates) {
			
			
			jdbcTemplate.batchUpdate(AMENDMENT_UPDATE_QUERY, new BatchPreparedStatementSetter() {

				@Override
				public void setValues(PreparedStatement ps, int rowNum) throws SQLException {

					AmendmentUpdate amendmentUpdate = amendmentUpdates.get(rowNum);
					AuditDetails auditDetails = amendmentUpdate.getAuditDetails();
					
					ps.setString(1, amendmentUpdate.getStatus().toString());
					ps.setString(2, amendmentUpdate.getAmendedDemandId());
					ps.setString(3, auditDetails.getLastModifiedBy());
					ps.setLong(4, auditDetails.getLastModifiedTime());
					ps.setObject(5, util.getPGObject(amendmentUpdate.getAdditionalDetails()));
					ps.setString(6, amendmentUpdate.getTenantId());
					ps.setString(7, amendmentUpdate.getAmendmentId());
				}

				@Override
				public int getBatchSize() {
					return amendmentUpdates.size();
				}

			});
		}
	}
