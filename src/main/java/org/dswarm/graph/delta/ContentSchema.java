package org.dswarm.graph.delta;

import java.util.LinkedList;

import org.dswarm.graph.delta.deserializer.ContentSchemaDeserializer;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * Created by tgaengler on 29/07/14.
 */
@JsonDeserialize(using = ContentSchemaDeserializer.class)
public class ContentSchema {

	@JsonProperty("record_identifier_attribute_path")
	private AttributePath				recordIdentifierAttributePath;

	@JsonProperty("key_attribute_paths")
	private LinkedList<AttributePath>	keyAttributePaths;

	@JsonProperty("value_attribute_path")
	private AttributePath				valueAttributePath;

	public ContentSchema(final AttributePath recordIdentifierAttributePathArg, final LinkedList<AttributePath> keyAttributePathsArg,
			final AttributePath valueAttributePathArg) {

		recordIdentifierAttributePath = recordIdentifierAttributePathArg;
		keyAttributePaths = keyAttributePathsArg;
		valueAttributePath = valueAttributePathArg;
	}

	public AttributePath getRecordIdentifierAttributePath() {

		return recordIdentifierAttributePath;
	}

	public LinkedList<AttributePath> getKeyAttributePaths() {

		return keyAttributePaths;
	}

	public AttributePath getValueAttributePath() {

		return valueAttributePath;
	}

	@Override
	public boolean equals(Object o) {

		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ContentSchema that = (ContentSchema) o;

		if (keyAttributePaths != null ? !keyAttributePaths.equals(that.keyAttributePaths) : that.keyAttributePaths != null) {
			return false;
		}
		if (recordIdentifierAttributePath != null ? !recordIdentifierAttributePath.equals(that.recordIdentifierAttributePath)
				: that.recordIdentifierAttributePath != null) {
			return false;
		}
		if (valueAttributePath != null ? !valueAttributePath.equals(that.valueAttributePath) : that.valueAttributePath != null) {
			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {

		int result = recordIdentifierAttributePath != null ? recordIdentifierAttributePath.hashCode() : 0;
		result = 31 * result + (keyAttributePaths != null ? keyAttributePaths.hashCode() : 0);
		result = 31 * result + (valueAttributePath != null ? valueAttributePath.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {

		// TODO:

		return null;
	}

}
