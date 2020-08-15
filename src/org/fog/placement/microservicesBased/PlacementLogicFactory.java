package org.fog.placement.microservicesBased;

import org.fog.test.Optimizer.QoSAwareOptimizationPlacementLogic;
import org.fog.utils.Logger;

/**
 * Created by Samodha Pallewatta.
 */
public class PlacementLogicFactory {

    public static final int EDGEWART_MICROSERCVICES_PLACEMENT = 1;
    public static final int QOS_AWARE_OPL = 2;

    public MicroservicePlacementLogic getPlacementLogic(int logic, int fonId) {
        switch (logic) {
            case EDGEWART_MICROSERCVICES_PLACEMENT:
                return new EdgewardMicroservicePlacementLogic(fonId);
            case QOS_AWARE_OPL:
                return new QoSAwareOptimizationPlacementLogic(fonId);
        }

        Logger.error("Placement Logic Error", "Error initializing placement logic");
        return null;
    }

}
