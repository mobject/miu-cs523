import java.io.Serializable;
import java.util.Map;

public class NeoWSData implements Serializable {
    private Map<String, Object> near_earth_objects;

    public Map<String, Object> getNear_earth_objects() {
        return near_earth_objects;
    }

    public void setNear_earth_objects(Map<String, Object> near_earth_objects) {
        this.near_earth_objects = near_earth_objects;
    }

    @Override
    public String toString() {
        return "NeoWSData{" +
                "near_earth_objects=" + near_earth_objects.size() +
                '}';
    }
}
