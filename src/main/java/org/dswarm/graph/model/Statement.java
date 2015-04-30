/**
 * This file is part of d:swarm graph extension.
 *
 * d:swarm graph extension is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * d:swarm graph extension is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with d:swarm graph extension.  If not, see <http://www.gnu.org/licenses/>.
 */
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
	private final Optional<Long>              optionalResourceHash;
	private final Optional<Map<String, Object>> optionalQualifiedAttributes;

	public Statement(final Optional<NodeType> optionalSubjectNodeType, final Optional<String> optionalSubjectURI,
			final Optional<String> optionalSubjectId, final Optional<String> optionalSubjectDataModelURI,
			final Optional<String> optionalPredicateURI, final Optional<NodeType> optionalObjectNodeType, final Optional<String> optionalObjectURI,
			final Optional<String> optionalObjectValue, final Optional<String> optionalObjectId, final Optional<String> optionalObjectDataModelURI,
			final Optional<String> optionalStatementUUID, final Optional<Long> optionalResourceHash,
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
		this.optionalResourceHash = optionalResourceHash;
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

	public Optional<Long> getOptionalResourceHash() {
		return optionalResourceHash;
	}

	public Optional<Map<String, Object>> getOptionalQualifiedAttributes() {
		return optionalQualifiedAttributes;
	}
}
