package org.dswarm.graph.model;

import java.util.Map;

import com.google.common.base.Optional;

import org.dswarm.graph.NodeType;

/**
 * @author tgaengler
 */
public final class Statement {

	private final Optional<NodeType>			optionalSubjectNodeType;
	private final Optional<String>				optionalSubjectURI;
	private final Optional<String>				optionalSubjectId;
	private final Optional<String>				optionalSubjectDataModelURI;
	private final Optional<String>				optionalPredicateURI;
	private final Optional<NodeType>			optionalObjectNodeType;
	private final Optional<String>				optionalObjectURI;
	private final Optional<String>				optionalObjectValue;
	private final Optional<String>				optionalObjectId;
	private final Optional<String>				optionalObjectDataModelURI;
	private final Optional<String>				optionalStatementUUID;
	private final Optional<String>				optionalResourceURI;
	private final Optional<Map<String, Object>>	optionalQualifiedAttributes;

	public Statement(final Optional<NodeType> optionalSubjectNodeType, final Optional<String> optionalSubjectURI,
			final Optional<String> optionalSubjectId, final Optional<String> optionalSubjectDataModelURI,
			final Optional<String> optionalPredicateURI, final Optional<NodeType> optionalObjectNodeType, final Optional<String> optionalObjectURI,
			final Optional<String> optionalObjectValue, final Optional<String> optionalObjectId, final Optional<String> optionalObjectDataModelURI,
			final Optional<String> optionalStatementUUID, final Optional<String> optionalResourceURI,
			final Optional<Map<String, Object>> optionalQualifiedAttributes) {

		this.optionalSubjectNodeType = optionalSubjectNodeType;
		this.optionalSubjectURI = optionalSubjectURI;
		this.optionalSubjectId = optionalSubjectId;
		this.optionalSubjectDataModelURI = optionalSubjectDataModelURI;
		this.optionalPredicateURI = optionalPredicateURI;
		this.optionalObjectNodeType = optionalObjectNodeType;
		this.optionalObjectURI = optionalObjectURI;
		this.optionalObjectValue = optionalObjectValue;
		this.optionalObjectId = optionalObjectId;
		this.optionalObjectDataModelURI = optionalObjectDataModelURI;
		this.optionalStatementUUID = optionalStatementUUID;
		this.optionalResourceURI = optionalResourceURI;
		this.optionalQualifiedAttributes = optionalQualifiedAttributes;
	}

	public Optional<NodeType> getOptionalSubjectNodeType() {
		return optionalSubjectNodeType;
	}

	public Optional<String> getOptionalSubjectURI() {
		return optionalSubjectURI;
	}

	public Optional<String> getOptionalSubjectId() {
		return optionalSubjectId;
	}

	public Optional<String> getOptionalSubjectDataModelURI() {
		return optionalSubjectDataModelURI;
	}

	public Optional<String> getOptionalPredicateURI() {
		return optionalPredicateURI;
	}

	public Optional<NodeType> getOptionalObjectNodeType() {
		return optionalObjectNodeType;
	}

	public Optional<String> getOptionalObjectURI() {
		return optionalObjectURI;
	}

	public Optional<String> getOptionalObjectValue() {
		return optionalObjectValue;
	}

	public Optional<String> getOptionalObjectId() {
		return optionalObjectId;
	}

	public Optional<String> getOptionalObjectDataModelURI() {
		return optionalObjectDataModelURI;
	}

	public Optional<String> getOptionalStatementUUID() {
		return optionalStatementUUID;
	}

	public Optional<String> getOptionalResourceURI() {
		return optionalResourceURI;
	}

	public Optional<Map<String, Object>> getOptionalQualifiedAttributes() {
		return optionalQualifiedAttributes;
	}
}
