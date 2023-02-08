package nph.aspect;

import org.apache.log4j.Logger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

@Aspect
public class LogAspect {

    private static Logger log = Logger.getLogger(LogAspect.class);

    @Around("@annotation(nph.aspect.ExecutionTime) && execution(* *(..))")
    public Object logStartTime(ProceedingJoinPoint joinPoint) {
        log.info("Method " + joinPoint.getSignature().getName() + "() starting...");
        long start = System.currentTimeMillis();
        try {
            Object object = joinPoint.proceed();
            long end = System.currentTimeMillis();
            log.info("Method " + joinPoint.getSignature().getName() + "() exeTime: " + (end - start) / 1000 + "s");
            return object;
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return null;
    }


}
