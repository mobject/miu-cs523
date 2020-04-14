package edu.miu.cs523.neows;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class NeoWSData implements Serializable {
    Map<String, Object> near_earth_objects;
}
