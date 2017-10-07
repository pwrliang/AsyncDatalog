package socialite.async.util;

import mpi.MPI;
import mpi.MPIException;

public class MPIUtils {
    public static boolean inMPIEnv() {
//        return false;
        try {
            MPI.COMM_WORLD.Rank();
        } catch (MPIException e) {
            return false;
        }
        return true;
    }
}
